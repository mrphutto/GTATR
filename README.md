# GTATR V1.6 (GIS Tools and All The REST)
Python library to query ESRI ArcGIS REST services and get results in JSON.

# Notable Functions include:

+ Bulk download to overcome the online server limits 
+ Portal/token authentication
+ Convert ESRI geometry to geoJSON format
+ Save attribute data to .csv
+ Save features with geometry as .geojson with all attribute data	

The main library gtatr.py just uses the requests library for simplicity, nothing else required. No ESRI or other third party libraries.

An example driver RestDownloader.py extends functionality to include .csv and geojson support. It also allows multiple REST endpoint links to be loaded in via a .csv for batch downloading.

![QGIS Snip](https://github.com/pathutto/images/blob/master/QGIS_Snip1.PNG?raw=true)
