# TODO

# Importing geodata
There are several methods to make layers available on PostgreSQL. See official documentation [here](https://docs.qgis.org/3.22/en/docs/training_manual/spatial_databases/import_export.html?). DB Manager is probably the easiest way. A fast way would be to use the ["Export to PostgreSQL"](https://docs.qgis.org/3.22/en/docs/user_manual/processing_algs/qgis/database.html) tool.

# Data Pipeline
This section documents the data manipulation to achieve different layers.

## address_point_bbl
1. Use "Join attributes by location" tool to match bbl field of underlying *mappluto* layer with that of *address_point*.
2. Convert the bbl field to text with 0 precision under the layer properties.