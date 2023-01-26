"""
Holds backend for django project yoast.
"""

import pandas as pd
import usaddress

from .postgres import Postgres
from .utils.globals import BOROUGH, remap, QUEENS
from .utils.mydifflib import get_close_matches_indexes


class Query:
    '''
    Query takes in a full address (ex: 225 W 86th St, New York, NY 10024), and finds a match in the MapPluto table. The
    input can be a bit messy, e.g. missing puncutations and spelling errors, and it will still try to find a match. 
    
    A hard requirement is the inclusion of correct city and zip code because the query specifies borough and zip code to 
    run a select SQL query for faster matching.
    '''
    _num_matches = 5
    
    def __init__(self, full_address):
        self.psql = Postgres('qgis')
        
        self._scores = []
        self._queries = []

        # retrieve parsed address fields
        address_dict = usaddress.tag(full_address.upper())

        # streamline address string
        fid = [
            "BuildingName",
            "AddressNumber",
            "StreetNamePreDirectional",
            "StreetName",
            "StreetNamePostType",
        ]
        address = ""
        for i in fid:
            if i in address_dict[0].keys():
                address = address + address_dict[0][i] + " "

        # streamline boro string
        try:
            boro = address_dict[0]["PlaceName"]
        except KeyError:
            raise ValueError("Include the 'City' field id.")
        if boro in QUEENS:
            boro = "QN"
        elif boro in BOROUGH.keys() or boro in BOROUGH.values():
            if boro in BOROUGH.keys():
                boro = BOROUGH[boro]
        else:
            raise ValueError("Make sure 'City' field id is correct. Received: " + boro)

        # defining zip code string
        zipcode = address_dict[0]["ZipCode"]

        self.matches = self.query(address, boro, zipcode)
        self._scores.extend(self.matches[0])
        self._queries.append(self.matches[1])

        self.psql.close()

    def query(self, address, boro, zipcode):
        """Returns server data as pandas.DataFrame."""
        table_name = 'mappluto_unclipped'
        params = {"boro": boro, "zipcode": zipcode}
        
        statement = "select * from {} where borough = %(boro)s and zipcode = %(zipcode)s;".format(
            self.psql.schema_name + '.' + table_name
        )
        data = self.psql.get(table_name, statement=statement, params=params)
        self._all_results = data
        self._all_addresses = data['address'].dropna()
        
        if data.empty:
            raise ValueError("Incorrect zip code. Enter a NYC zip code.")

        # remove nulls since data["address"] contains ~ 500 null values
        data.dropna(subset=["address"], inplace=True)
        data.reset_index(drop=True, inplace=True)
        match = get_close_matches_indexes(address, data["address"], n=self._num_matches)
        
        # rearrange match results so that scores are in one list and indexes in another
        zipped_match = list(zip(*match))
        return (zipped_match[0], remap(data.iloc[list(zipped_match[1])]))
