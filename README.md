# GTATR V1.6 (GIS Tools and All The REST)
Library to query ArcGIS REST services and get results in Python

Notable Functions include:
*Bulk download to overcome the feature caching limits
*Portal/token authentication
*Convert ESRI geometry to geoJSON format
*Save attribute data to .csv
*Save features with geometry as .geojson with all attribute data

The main library gtatr.py just uses requests library for simplicity

An example driver RestDownloader.py "Driver" extends functionality to include .csv and geojson support. It also has a bulk REST input for download by filling out a .csv file with REST endpoints.

![QGIS Snip](https://github.com/pathutto/images/blob/master/QGIS_Snip1.PNG?raw=true)
