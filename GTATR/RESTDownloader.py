import gtatr as gt
import json
import geojson
from geojson import Point, Feature, FeatureCollection, dump
import csv
import os

#This script is used in conjunction with the GTATR library for
#downloading and converting online data from ArcGIS REST Services
#It uses some libraries such as json, geojson, and csv for 
#data formatting

#this function will use GTATR to download features in bulk and save to a .geojson file
def downloadFeaturesAsGeoJSON(RConnect, baseURL, queryText, attributes, outName):
    
    #First step is to set the token so we have it for future calls
    #example "VukprqQVq_FG477WjsM4uS8txu6XdZGkpsDsDCoYns8."
    #example baseURL "http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5"
    #example queryText "STATE_NAME = 'Alaska'"
    #example attributes 'MALES, FEMALES, MED_AGE'
    #example outName 'states.geojson'
        
    #set the Feature Layer URL and headers including the token for authentication

    #create the full URL, e.g."http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5"
    URL = baseURL + "/query?"    
    
    #get a list of features back from REST response
    featuresWithGeometry = RConnect.getFeaturesWithGeometry(URL, attributes, queryText) 

    #convert the ESRI style geometry to the geojson standard
    print("Data Downloaded, saving..")           
    GeoJSON = RConnect.convertESRIGeometry(featuresWithGeometry)    
    
    #save the result out to a .geojson file
    with open(outName, 'w') as f:
       dump(GeoJSON, f)

    print("Complete with " + outName)

#this function will use GTATR to download features in bulk and save to a .csv file (no geometry)
def downloadFeaturesAsCSV(RESTConnect, baseURL, queryText, attributes, outName, isDataTable=False):
    
    #First step is to set the token so we have it for future calls
    #example "VukprqQVq_FG477WjsM4uS8txu6XdZGkpsDsDCoYns8."
    #example baseURL "http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5"
    #example queryText "STATE_NAME = 'Alaska'"
    #example attributes 'MALES, FEMALES, MED_AGE'
    #example outName 'states.geojson'

    #set the Feature Layer URL and headers including the token for authentication

    #create the full URL, e.g."http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/"
    URL = baseURL + "/query?"    
    
    if isDataTable:
        features = RESTConnect.getTableData(URL, attributes, queryText) 
    else:
        #get a list of features back from REST response
        features = RESTConnect.getFeaturesWithGeometry(URL, attributes, queryText) 
   # print(featuresAsGEOJSON) 
 
    csv_columns = []


    #try to convert an x y to something
    for f in features:

        geo = f["geometry"]

        #try point
        try:
            geoX = geo["x"]
            geoY = geo["y"]

            f["ConvertedX"] = geoX
            f["ConvertedY"] = geoY
        except Exception as ex:
            print("not a point, not mapping lat/longs")   
            break

    #map the feature attributes to a list of columns for the output csv
    for key in features[0].keys():
        csv_columns.append(key)

    #write the table data to a .csv using DictWriter
    with open(outName, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in features:
                    writer.writerow(data)

    print("Complete with " + outName)

#this function will use a .csv to batch download from multiple REST endpoints
def batchDownloadFromCSV(inputcsv):
        
    #create a list to store the map links that are to be downloaded
    mapList = []

    #attempt to open the .csv and read entries into list
    with open(inputcsv, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
    
        #loop through all rows in the .csv
        for row in csv_reader:
        
            #the first line grabs the column names
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1

            #create a dictionary and append to the maplist
            mapDict = { "fileName" : row["OutName"] + ".geojson",
                       "MapServerLink": row["MapServerLink"]}

            #add this item to the queue
            mapList.append(mapDict)
        
            #move to next row and increase counter
            line_count += 1

    #now run through all the maps to download
    for map in mapList:
        coreLink = ""

        #parse the REST endpoint URL to get the URL required for access
        coreLink = map["MapServerLink"][:map["MapServerLink"].find("/rest/")]
       # print(coreLink)

        #use the GTATR libary to download the data as geojson, getting all features and attributes
        RConnect = gt.RESTConnector(coreLink)
        RConnect.setToken("") #no token needed
        downloadFeaturesAsGeoJSON(RConnect, map["MapServerLink"], 
                     "1=1", 
                     '*',
                     map["fileName"])
 
    print("Complete with batch download...")

#Main method to show some practical examples of using the library
if __name__ == "__main__":

    #The first example is a really simple example of downloading REST data to a .csv
    print("Example of downloading REST data as a table into a .csv")

    #Initalize the GTATR library by giving the base REST url
    RConnect = gt.RESTConnector("http://sampleserver1.arcgisonline.com/ArcGIS")
    
    #First step is to set the token so we have it for future calls
    #this example doesn't need a token - but options for getting a token to authenticate are below
    #RConnect.setToken("VukprqQVq_FG477WjsM4uS8txu6XdZGkpsDsDCoYns8.")
    #tokenNo = RConnect.getRESTToken("username", "password", "https://mydomain.net/portal/sharing/rest/generateToken")

    #then set query parameters for example "STATE_NAME = 'Alaska'"
    queryText = "1=1" #get all features

    #set the attributes desired seperated by commas - as for example 'MALES, FEMALES, MED_AGE'
    attributes = '*' #get all attributes

    ##We will call the function with the requested URL and give it our parameters and an output .csv name
    #downloadFeaturesAsCSV(RConnect, "http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5", 
    #                 queryText, 
    #                 attributes, 
    #                 'States.csv')

    ##Using most of the same parameters, we can also output with geometry as a .geojson
    #downloadFeaturesAsGeoJSON(RConnect, "http://sampleserver1.arcgisonline.com/ArcGIS/rest/services/Demographics/ESRI_Census_USA/MapServer/5", 
    #                 queryText, 
    #                 attributes, 
    #                 'States.geojson')

    dataDirectory = (os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + r"/data"

       #Using most of the same parameters, we can also output with geometry as a .geojson
    downloadFeaturesAsGeoJSON(RConnect, " https://services2.arcgis.com/pWwHCwVEtR8QoS9x/ArcGIS/rest/services/Uniti_Fiber/FeatureServer/0", 
                     queryText, 
                     attributes, 
                     dataDirectory + r"/uniti.geojson")


    batchDownloadFromCSV(dataDirectory + r"/maplinks.csv")
