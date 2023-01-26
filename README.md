# TODO

## Before production
1. Currently, importing any geodata via [QGIS](gis/QGIS%20Setup.md). Goal is to add feature to import the following datasets to **vukevint/nycdb**:
   - mappluto
      - See [here](https://spatial-dev.guru/2021/10/30/import-shapefile-to-postgresql-postgis-database-using-geopandas-python/) and [item 2 here](https://blog.devgenius.io/3-easy-ways-to-import-a-shapefile-into-a-postgresql-database-c1a4c78104af)
      - [download link](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page)
      - Make sure to import such that bbl is a character. Match that of the annualsales
   - subway station
      - [open data download link](https://data.cityofnewyork.us/Transportation/Subway-Stations/arq3-7z49)
2. Allow **vukevint/nycdb** to accept configparser.
   - See notes in *cli* module.
3. Implement Docker.

# housing_nyc: platform for analysis of public NYC data

## Introduction
**housing_nyc** processes openly available real estate data, which is imported with **vukevint/nycdb**. Datasets from here are stored on a postgresql relation (default: *public.nycdb*), and any processed data is uploaded to other schemas.

Juypter Lab is used at times for data exploration, as well as development. The predecessor to this library, **real-estate-nyc**, first started as a means to tread these waters. It eventually became messy, and is rewritten into **housing-nyc**.

**housing_nyc** is used as the backend for the Django project **yoast**. Relations are queried and useful tables are created by **housing_nyc** to support the data flow of **yoast**.

## Goals
There are many ideas and projects that are feasible with the available data. Here is a running list:
- property value modelling
- subway ranking
- graph of subway stations

# Set Up

## Assumed requirements
1. PostgreSQL is running properly.
2. *nycdb* database is created. Note that **vukevint/nycdb** will create one if it is not available.
3. PostGIS extension is set in the *nycdb*. Note that **vukevint/nycdb** does not create the PostGIS extension automatically as of 2022-07-17.

## config.ini
If this library is cloned, config.ini file should be available in the root folder.
```
housing-nyc
│   README.md
│   config.ini <---
│   ...
│
└───housing_nyc
│   │   file011.py
│   │   file012.py
│   │   ...
```
It is recommended to use the default dbname and schemas as data pipelines can be broken if they are changed.

The connection parameters should be edited based on your PostgreSQL setup.

```
[nycdb]
host = insert_host
port = insert_port
user = insert_username
password = insert_password
dbname = nycdb
```

## Downloading standard set
Use nycdb to download the following dataset:
- rolling sales
- property assessment values
- mappluto (tba)
- subway 
May want to create bash script that is run once.

## Import geodata with QGIS
[See this file on how to import data to PostgreSQL.](gis/QGIS%20Setup.md) Note that QGIS is used mostly for import raw geodata, and for QAQC. Visualizations, maps, data processing, and analysis is done through Python.