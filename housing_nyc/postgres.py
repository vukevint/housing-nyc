""" 
Manage relational tables from postgres database.

TODO:
- create information schema class to find all available tables/views
in Postgres connection
    - can edit Postgres._table_exists to check through this list of tables/views
"""

from functools import wraps
from pathlib import Path
from configparser import ConfigParser
from psycopg2 import sql
import pandas as pd
import psycopg2
import io

from .utils.paths import config_path
from .utils.globals import CONFIG_INI_SECTION


def reconnect(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        cls = args[0]
        if not cls.connected():
            cls.connect()
        output = func(*args, **kwargs)
        cls.close()
        return output
    return wrapper


class Postgres:
    """
    Base class for managing postgresql database connection. All tables are assumed to be pandas.DataFrame

    Parameters
    ----------
    section : str
        Section name in config.ini

    Section Options
    ---------------
    'nycdb'
        tables imported by nycdb
    'qgis'
        tables requiring PostGIS extensions, or containing geom
    """
    def __init__(self, section):
        if self._section_exists(section):
            self._section_name = CONFIG_INI_SECTION[section]  
        else:
            # TODO: include valid section names from CONFIG_INI_SECTION in error message
            raise ValueError("'" + str(section) + "'" + ' is not a valid section keyword argument.')
        self.default_column_type = 'text'
        self.schema_name = 'public'
        self.connect()

    def _section_exists(self, section):
        return section in CONFIG_INI_SECTION.keys()

    def _table_exists(self, table_name):
        exists = False

        try:
            if self.schema_name != 'public':
                statement = ("select exists(select from information_schema.tables where table_schema='" 
                             + self.schema_name + "' AND table_name='" + table_name + "');")
                cur = self.execute(statement)
            else:
                statement = ("select exists(select relname from pg_class where relname='" + table_name + "');")
                cur = self.execute(statement)
            exists = cur.fetchone()[0]
        except psycopg2.Error as e:
            print(e)
        return exists

    def connect(self, section_name=None):
        if section_name is None:
            section_name = self._section_name
        
        self.conn = self.get_connection_by_config(section_name)

    def connected(self) -> bool:
        return self.conn and self.conn.closed == 0

    def get_connection_by_config(self, section_name):
        """
        This method will connect with the login parameters as specified by the 
        section argument in the class instansiation.

        Returns
        -------
        conn : psycopg2.connection
        """
        config_parser = ConfigParser()
        config_parser.read(config_path)
        
        if config_parser.has_option(section_name, "schema"):
            self.schema_name = config_parser[section_name]["schema"] 

        return psycopg2.connect(
            host=config_parser[section_name]["host"],
            port=config_parser[section_name]["port"],
            user=config_parser[section_name]["user"],
            password=config_parser[section_name]["password"],
            dbname=config_parser[section_name]["dbname"],
        )

    def _cursor(self):
        return self.conn.cursor()

    def close(self):
        if self.connected():
            self.conn.close()

    def _transaction(self):
        return self.conn

    def execute(self, statement, parameters=None):
        with self._transaction():
            cur = self._cursor()
            try:
                if parameters is None:
                    cur.execute(statement)
                else:
                    cur.execute(statement, parameters)
            except:
                cur.close()
                raise
            return cur
    
    def _full_table_name(self, schema, table):
        return '{}.{}'.format(schema, table)

    def get(self, table_name, statement=None, params=None):
        """
        Imports table into pandas.DataFrame.
        
        Has an option to form a query statement, else uses standard "select * from table_name"

        Parameters
        ----------
        table_name : str
            Name of table in database
        statement : str; optional
            Custom query statement
        params : list, dict, tuples; optional
            pandas.read_sql params argument

        Returns
        -------
        df : pandas.DataFrame
            Imported table
        """
        if statement:
            # check if queried relation contains schema prefix
            check_schema = statement.replace('\n', '').split('from ')[1].strip()
            if not ('.' in check_schema and check_schema.split('.')[0] == self.schema_name):
                print("Error: Schema name in query statement does not match schema in config.ini.")
                return
        else:
            statement = "select * from {};".format(self._full_table_name(self.schema_name, table_name))
        if self._table_exists(table_name):
            df = pd.read_sql_query(statement, self.conn, params=params)
            return df
        else:
            print('Table \'{}\' does not exist.'.format(table_name))

    def upload(self, table, table_name, schema=None):
        if schema is None:
            schema = self.schema_name

        # table columns for sql statement
        cols = tuple((s, self.default_column_type) for s in list(table.columns))        
        fields = []
        for col in cols:
            fields.append(sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])))

        _table_name = self._full_table_name(schema, table_name)

        # create empty table
        self.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {} ({})').format(
                     sql.SQL(_table_name),
                     sql.SQL(',').join(fields)
                     ))
        
        # from https://stackoverflow.com/a/47984180
        # decided against pd.to_sql because it requires sqlalchemy
        # will have to check if it can overwrite
        cur = self._cursor()
        output = io.StringIO()
        table.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_expert('COPY ' + _table_name + ' FROM STDIN', output)
        self.conn.commit()
        cur.close()

        print('Uploaded table "{}" in schema "{}"'.format(table_name, schema))


class BblBldg(Postgres):
    """Imports, processes, and joins MapPluto and property assessment datasets."""
    
    bblbldg_name = 'bblbldg'

    def __init__(self, mappluto='', avroll=''):
        # check if bbl_bldg table exists
        super().__init__('nycdb')
        if self._table_exists(self.bblbldg_name):
            print('Table already exists')
            return
        if mappluto:
            self.mappluto_name = mappluto
        else:
            self.mappluto_name = 'mappluto'
        if avroll:
            self.avroll_name = avroll
        else:
            self.avroll_name = 'avroll'

        self.qgis = Postgres('qgis')

    def _get_tables(self):
        query = '''select 
                replace(replace(replace(replace(replace(borough,'MN','1'),'BX','2'),'BK','3'),'QN','4'),'SI','5') as boro, 
                lpad(block::text, 5, '0') as block, 
                lpad(lot::text, 4, '0') as lot, 
                bbl::text,
                address, 
                zipcode::text
                from {}.{}'''.format(self.qgis.schema_name, self.mappluto_name)
        self.mappluto = self.qgis.get(self.mappluto_name, statement=query)
        # self.mappluto.set_index(['boro', 'block', 'zipcode'], inplace=True)

        query = '''select 
                boro::text,
                block::text,
                lot::text,
                bbl::text,
                housenum_lo::text,
                housenum_hi::text,
                street_name::text,
                aptno::text,
                zip_code::text 
                from {}.{}'''.format(self.schema_name, self.avroll_name)
        self.avroll = self.get(self.avroll_name, statement=query)
        self.avroll['address'] = self.avroll['housenum_lo'] + ' ' + self.avroll['street_name']  # create address col to match mappluto
        # self.avroll.set_index(['boro', 'block', 'zip_code'], inplace=True)

    def _join_tables(self):
        dfmerge = self.avroll.merge(self.mappluto, left_on=['boro', 'block', 'zip_code'], right_on=['boro', 'block', 'zipcode'])
        compare_address = (dfmerge['address_x'] == dfmerge['address_y'])
        return dfmerge.loc[compare_address[compare_address == 1].index]

    def build_table(self):
        self._get_tables()
        self.bblbldg = self._join_tables()
