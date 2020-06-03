import gtatr as gt
import json
import geojson
from geojson import Point, Feature, FeatureCollection, dump
import csv


def downloadFeaturesAsGeoJSON(RConnect, baseURL, queryText, attributes, outName):


    #First step is to set the token so we have it for future calls
    #example "VukprqQVq_FG477WjsM4uS8txu6XdZGkpsDsDCoYns8."
    #example baseURL "https://fhdwpicgva023.verizon.com/arcgis/rest/services/VerizonTelecom/MapServer/1/"
    #example queryText "MARKETID = '69'"
    #example attributes 'Internal_Permit_ID, OBJECTID, APPROVING_AUTHORITY_NAME, APPROVING_AUTHORITY_NUMBER'
    #example outName 'arksplice.geojson'

    #set the Feature Layer URL and headers including the token for authentication

    #create the full URL, e.g."https://fhdwpicgva023.verizon.com/arcgis/rest/services/VerizonTelecom/MapServer/1/query?"
    URL = baseURL + "/query?"    
    #First part is to get the list of object ids and break them into small pieces
    chunks = RConnect.getOIDsFromService(URL, queryText)   
    print("Got Chunks")

    #get a list of features back from REST response
    featuresWithGeometry = RConnect.getFeaturesWithGeometry(URL, chunks, attributes, queryText) 

    print("Data Downloaded, saving..")
           
    testGeoJSON = RConnect.convertESRIGeometry(featuresWithGeometry)    
 
    with open(outName, 'w') as f:
       dump(testGeoJSON, f)

    print("Complete with " + outName)


def downloadFeaturesAsCSV(RESTConnect, baseURL, queryText, attributes, outName, isDataTable=False):


    #First step is to set the token so we have it for future calls
    #example "VukprqQVq_FG477WjsM4uS8txu6XdZGkpsDsDCoYns8."
    #example baseURL "https://fhdwpicgva023.verizon.com/arcgis/rest/services/VerizonTelecom/MapServer/1/"
    #example queryText "MARKETID = '69'"
    #example attributes 'Internal_Permit_ID, OBJECTID, APPROVING_AUTHORITY_NAME, APPROVING_AUTHORITY_NUMBER'
    #example outName 'arksplice.csv'

    #set the Feature Layer URL and headers including the token for authentication


    #create the full URL, e.g."https://fhdwpicgva023.verizon.com/arcgis/rest/services/VerizonTelecom/MapServer/1/query?"
    URL = baseURL + "/query?"    

    
    if isDataTable:
        features = RESTConnect.getTableData(URL, attributes, queryText) 
    else:
        #First part is to get the list of object ids and break them into small pieces
        chunks = RESTConnect.getOIDsFromService(URL, queryText)   
        print("Got Chunks")

        #get a list of features back from REST response
        features = RESTConnect.getFeaturesWithGeometry(URL, chunks,  attributes, queryText) 
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
            print("not a point")
        

    for key in features[0].keys():
        csv_columns.append(key)

    with open(outName, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for data in features:
                    writer.writerow(data)

    print("Complete with " + outName)

if __name__ == "__main__":


    
    #RConnect = gt.RESTConnector('https://maps.foresitegroup.net/arcgis')

    #my_Token = RConnect.getRESTToken("Utopia",'roksUtopia101!','https://maps.foresitegroup.net/arcgis/tokens/') 
    #RConnect.setToken(my_Token)

    #URL = 'https://maps.foresitegroup.net/arcgis/rest/services/Utopia/Utopia_Idaho_East/MapServer/7/query?'

    #queryText = "SOURCE_CABLE_NAME = 'IDF013.2.1'"

    # #First part is to get the list of object ids and break them into small pieces
    #chunks = RConnect.getOIDsFromService(URL, queryText)   

    #attributes = "*"

    ##get a list of features back from REST response
    #featuresWithGeometry = RConnect.getFeaturesWithGeometry(URL, chunks, attributes, queryText) 

    #print(featuresWithGeometry)

    
  

    #exit()

    mapList = []

       #attempt to open the .csv and read entries into list
    with open('maplinks.csv', mode='r') as csv_file:
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

            mapList.append(mapDict)
        
            #move to next row and increase counter
            line_count += 1

    for map in mapList:
        coreLink = ""

        coreLink = map["MapServerLink"][:map["MapServerLink"].find("/rest/")]
       # print(coreLink)

        RConnect = gt.RESTConnector(coreLink)
        RConnect.setToken("") #no token needed
        downloadFeaturesAsGeoJSON(RConnect, map["MapServerLink"], 
                     "1=1", 
                     '*',
                     map["fileName"])


 
    print("Complete")