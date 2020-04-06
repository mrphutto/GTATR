########################################
#GIS Tools and all the REST (GTATR) v1.3
########################################

import requests #REST requests

#This class is used for reading and bulk downloading data
#from ArcGIS REST Services, it uses regular rest requests
#and does not rely on any ESRI libraries. format is a list
#of Python dictionaries in most cases

class RESTConnector():
    """description of class"""
    
    
    #basic construction, creates basic startup for the future REST connections
    def __init__(self, baseURL, tokenNo = ""): #base url is like https://maps.foresitegroup.net/arcgis
        
        
        self.baseURL = baseURL
        self.tokenNo = tokenNo #optional, but if the user knows the token - can pass it in
        
        #set an initial placeholder for the headers - once we get a token this will be updated
        self.HEADERS = {'user-agent': 'my-app/0.0.1',
                'Authorization': "Bearer " + self.tokenNo}
   
    #small function to break a list into chunks
    def create_chunks(self, list_name, n):
        for i in range(0, len(list_name), n):
            yield list_name[i:i + n]

    #give a user the ability to set their own token
    def setToken(self, tokenNo):
        self.tokenNo = tokenNo

        self.HEADERS = {'user-agent': 'my-app/0.0.1',
                'Authorization': "Bearer " + tokenNo}

    #pass in the credentials for authentication, and should get an ArcGIS token
    def getRESTToken(self, username, password, tokenURL):

        #First step is to authenticate with REST Service and get a token for future calls
   
        # defining the api-endpoint 
        #tokenURL = "https://maps.foresitegroup.net/arcgis/tokens/"

        # data to be sent to api 
        PARAMS = {'username':username, 
                'password':password,
                'client' : 'requestip', #this is critical for authentication, other ways dont work
                'referer': self.baseURL + "/" + "rest",
                'format' : 'JSON',
                'expiration': 60,} 
 
        # sending post request and saving response as response object 
        r = requests.post(url = tokenURL, data = PARAMS) 


        # extracting response text  
        tokenNo = r.text 
        print("The token is:%s"%tokenNo)

        self.setToken(tokenNo)

        return tokenNo

    #returns a list of object IDs from a given Feature Layer URL, this will return results over the 5K limit
    def getOIDsFromService(self, URL, queryText):      
      

        # data to be sent to api - "where 1=1" pulls all features, we want objectIDs only
        PARAMS = {'f':'pjson', 
                'where':queryText,
                'outSr' : '4326',
                'returnIdsOnly':'true',
                'returnGeometry' : 'false',
                'orderByFields' : 'OBJECTID ASC',
                'token' : self.tokenNo
              } 

        #get the objects ids from the REST response
        r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
        # print("respones : " + str(r.text))
        print("Reading OIDs...")
        try:
            data = r.json() #make sure we do the parantheses or its acts a little weird
        except:
            print("JSON Response Error " + str(r) + " - - " + str(r.text))

        try:
            OIDs = data['objectIds']
        except:
            print("data with objectIDs, " + str(r) + " - - " + str(r.text))

        print("ObjectIDs Pulled:" + str(len(OIDs)))
    
        #set the chunkSize, and break the list into chunks
        chunkSize = 300 #lets try 300 at a time, maximum is 1000
        chunks = list(self.create_chunks(OIDs, chunkSize))

        print("Number of Chunks:" + str(len(chunks)))

        return chunks

    #using a subset of the total Features (chunks) will return a list of features from given URL with requested attributes
    def getFeatures(self,URL, chunks, fields, queryText):

        #initialize a master list, we will append each querie's features to this
        featureList = []

        #now we will iterate through all the chunks
        for chunk in chunks:
    
            #get the OIDs for start and end of each chunk
            chunkStart = chunk[0]
            chunkEnd = chunk[len(chunk)-1]
    
            print("Chunk Start:" + str(chunkStart) + " - Chunk End:" + str(chunkEnd))

           
            # data to be sent to api - constrained by start and end OID for chunk
            PARAMS = {'f':'json', 
                    'where':'OBJECTID>=' +str(chunkStart)+ " AND OBJECTID <=" + str(chunkEnd) + " AND " + queryText,
                    'outSr' : '4326',
                    'outFields':fields,
                    'returnGeometry' : 'false',
                    'token' : self.tokenNo
                  } 

            #make the request
            r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
    
            #get the data back and convert the JSON response into a dictionary
            data = r.json() #make sure we do the parantheses or its acts a little weird
            try:
                features = data['features']
            except:
                print("URL Requested: " + str(URL))
                print(data)

            #loop through all the results in this chunk and append to master list
            for feature in features:
                attributes = feature['attributes']
                featureList.append(attributes)
         
        print(str(len(featureList)) + " features pulled from REST Service")

        return featureList

    #sometimes there are no ObjectIDs or geometry, its just table data
    def getTableData(self, URL, fields, queryText):

        #initialize a master list, we will append each querie's features to this
        featureList = []
        
        # data to be sent to api - constrained by start and end OID for chunk
        PARAMS = {'f':'json', 
                'where': queryText,           
                'outFields':fields,
                'returnGeometry' : 'false',
                'token' : self.tokenNo
                } 

        #make the request
        r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
    
        #get the data back and convert the JSON response into a dictionary
        data = r.json() #make sure we do the parantheses or its acts a little weird
        features = data['features']

        #loop through all the results in this chunk and append to master list
        for feature in features:
            attributes = feature['attributes']
            featureList.append(attributes)
         
        print(str(len(featureList)) + " features pulled from ROK")

        return featureList

    #returns the feature Geometry along with the raw data results given a Feature Layer URL
    def getFeaturesWithGeometry(self,URL, chunks, fields, queryText):

        #initialize a master list, we will append each querie's features to this
        featureList = []

        #now we will iterate through all the chunks
        for chunk in chunks:
    
            #get the OIDs for start and end of each chunk
            chunkStart = chunk[0]
            chunkEnd = chunk[len(chunk)-1]
    
            print("Chunk Start:" + str(chunkStart) + " - Chunk End:" + str(chunkEnd))

 
           
            # data to be sent to api - constrained by start and end OID for chunk
            PARAMS = {'f':'json', 
                    'where':'OBJECTID>=' +str(chunkStart)+ " AND OBJECTID <=" + str(chunkEnd) + " AND " + queryText,
                    'outSr' : '4326',
                    'outFields':fields,
                    'returnGeometry' : 'true',
                    'token' : self.tokenNo
                  } 

            #make the request
            r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
    
            try:
                data = r.json() #make sure we do the parantheses or its acts a little weird
            except Exception as ex:
                print("Error Reading JSON Data -- " + str(ex) + "-- " +  str(r.status_code) + "--" + str(r.reason) + "--" + str(r.content) + "--" + r.text)
                print("URL " + str(URL) + " PARAMs " + str(PARAMS))
                data = None


            features = data['features']
                #loop through all the results in this chunk and append to master list
            for feature in features:      
                attributes = feature['attributes']
                #for the geometry - attributes is a dictionary with the attribute data, we will add a new key/value with the geometry
                attributes.update({'geometry' : feature['geometry']})
     
                featureList.append(attributes)

        print(str(len(featureList)) + " features pulled from REST Service")

        return featureList

    #return a geoJSON results from Feature Layer URL with requested attributes
    def getFeaturesAsGeoJSON(self,URL, chunks, fields, queryText):

        #initialize the starter file, just set to None for now
        geoJSON = None

        #now we will iterate through all the chunks
        for chunk in chunks:
    
            #get the OIDs for start and end of each chunk
            chunkStart = chunk[0]
            chunkEnd = chunk[len(chunk)-1]
    
            print("Chunk Start:" + str(chunkStart) + " - Chunk End:" + str(chunkEnd))
              
    
            # data to be sent to api - constrained by start and end OID for chunk
            PARAMS = {'f':'geojson', 
                    'where':'OBJECTID>=' +str(chunkStart)+ " AND OBJECTID <=" + str(chunkEnd) + " AND " + queryText,
                    'outSr' : '4326',
                    'outFields':fields,
                    'returnGeometry' : 'true',
                    'token' : self.tokenNo
                  } 

            #make the request
            r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
    
            #get the data back and convert the JSON response into a dictionary
            try:
                data = r.json() #make sure we do the parantheses or its acts a little weird
            except Exception as ex:
                print("Error Reading JSON Data -- " + str(ex) + "-- " +  str(r.status_code) + "--" + str(r.reason) + "--" + str(r.content) + "--" + r.text)
                print("URL " + str(URL) + " PARAMs " + str(PARAMS))
                data = None
       
            #check if this is the first chunk or subsequent part
            if geoJSON is None:   
                geoJSON = data #if this is the first chunk, initialize the geoJSON data
                try:
                    a = str(len(geoJSON["features"]))
                except Exception as ex:
                    print("Error loading reading features -- " + str(ex) + "-- " +  str(r.status_code) + "--" + str(r.reason) + "--" + str(r.content) + "--" + r.text)
                    print("URL " + str(URL) + " PARAMs " + str(PARAMS))

            else:
                #if this is a subsequent chunk, we just add new features -not all the data
                try:
                    featuresToAdd = data["features"] #get the "Features" part of the JSON
                    for feature in featuresToAdd:
                        geoJSON["features"].append(feature) #loop through and append new features to the master JSON
                except Exception as ex:
                    print("Error loading reading features -- " + str(ex) + "-- " +  r.text + " -- " + str(r.status_code) + "--" + str(r.reason) + "--" + str(r.content))



        try:
            print(str(len(geoJSON["features"])) + " geojson features pulled from REST Service")
        except:
            print("Bad or No geoJson returned")

       # print("geojson pulled from REST Service")
        return geoJSON

    #function for troubleshooting - given a URL and query parameters, can view the raw results
    def getTestQueryResponse(self, URL, queryText, fields):
      
        # data to be sent to api - "where 1=1" pulls all features
        PARAMS = {'f':'pjson', 
                'where':queryText,
                'outSr' : '4326',           
                'returnGeometry' : 'false',
                'outFields':fields,
                'orderByFields' : 'OBJECTID ASC',
                'token' : self.tokenNo
              } 
        
        #get the objects ids from the REST response
        r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
        print("response : " + str(r.text))    
        try:
            data = r.json() #make sure we do the parantheses or its acts a little weird
        except:
            print("JSON Response Error " + str(r) + " - - " + str(r.text))

    #This will do a spatial query on the REST service using ESRI geometry and relationship (e.g. inside a polygon)
    def getFeaturesFromServiceByGeometry(self,URL, queryText, geometryToUse, geometryType, spatialQueryType, fields):
       
        #initialize a master list, we will append each querie's features to this
        featureList = []

        #for geometry type, see http://resources.esri.com/help/9.3/arcgisengine/ArcObjects/esriGeometry/esriGeometryType.htm
        #geometry types can be esriGeometryPoint,esriGeometryLine,esriGeometryPolygon,esriGeometryPolyline


        #esriSpatialRelIntersects | esriSpatialRelContains | esriSpatialRelCrosses | esriSpatialRelEnvelopeIntersects | esriSpatialRelIndexIntersects | esriSpatialRelOverlaps | esriSpatialRelTouches | esriSpatialRelWithin

        # data to be sent to api - "where 1=1" pulls all features, we want objectIDs only
        PARAMS = {'f':'json', 
                'where': queryText, #queryText,
                'geometryType': geometryType, #geometryType,
                'inSR': '4326',
                'geometry' : geometryToUse,            
                'spatialRel' : spatialQueryType, #queryType
                'outSr' : '4326',
                'outFields':fields,
                'returnGeometry' : 'false',
                'orderByFields' : 'OBJECTID ASC',
                'token' : self.tokenNo
              } 
    
        #get the objects ids from the REST response
        r = requests.get(url = URL, headers = self.HEADERS, params = PARAMS )
        data = r.json() #make sure we do the parantheses or its acts a little weird   
        print(r.json())
        try:
            features = data['features']
        except:
            features = []
        #    print("Error - no features " + str(r) + " - - " + str(r.text))
    

        #loop through all the results in this query and append to master list
        for feature in features:
            attributes = feature['attributes']          
            featureList.append(attributes)

        print(str(len(featureList)) + " features pulled from REST Service")

        return featureList

    #This will take a list of returned features with geometry and try to convert to a different format
    def convertESRIGeometry(self, featureList):

        #default the geometrytype to something that will catch attention if an error
        geometryType = "unk"

        
        #get the first feature in the list as an example
        try:
            firstFeature = featureList[0]
            geo = firstFeature["geometry"]
        except:
            print("no geometry found, unable to convert")
            return None

        #check the geometry type first, try to guess point/line/poly
        try:
            x = geo["x"]
            geometryType = "Point"        
        
        except:
            print("Error getting X - probably not a point")

            #now lets check if a line
            try:              
                paths = geo["paths"][0]      
                geometryType = "LineString" #paths has an extra list

            except:
                print("Error getting paths - not a line either")

                #last shot - see if its a polygon
                try:
                    rings = geo["rings"]     
                    geometryType = "Polygon" #paths has an extra list

                except:
                    print("Error getting rings - not a line either")

        #set up the base structure for a geoJSON file
        geoJSONFile = {"type" : "FeatureCollection",
                       "features": []}

        #go through all the ESRI feature and add them to the geoJSON
        for feature in featureList:

            #set the geometry type based on the logic from above
            geometryInGeoJSON = {"type" : geometryType}

            if geometryType == "Point":
                #encode the lat long from ESRI JSON to geoJSON
                geometryInGeoJSON["coordinates"] = [feature["geometry"]["x"], feature["geometry"]["y"]]

            if geometryType == "LineString":
             #   features['geometry']['paths'] #returns a list

                try:
                    geometryInGeoJSON["coordinates"] = feature["geometry"]["paths"][0]
                except: # if it fails, but some dummy coordinates in
                    geometryInGeoJSON["coordinates"] = [[102.0, 0.0], [103.0, 1.0]]
                    print("Null geometry encountered")

            if geometryType == "Polygon":           

                try:
                    geometryInGeoJSON["coordinates"] = feature["geometry"]["rings"]
                except: #if it fails, but some dummy coordinates in
                    geometryInGeoJSON["coordinates"] = [[100.0, 0.0], [101.0, 0.0], [101.0, 1.0],[100.0, 1.0], [100.0, 0.0]]
                    print("Null geometry encountered")
   
            #the attribute data is really just our feature data, so set it as a varible
            propertiesInGeoJSON = feature
            #remove the ESRI geometry, no longer needed
            del propertiesInGeoJSON["geometry"]

            #finalize the geoJSON format
            featureInGeoJSON = {"type" : "Feature", 
                                "geometry" : geometryInGeoJSON, 
                                "properties" :propertiesInGeoJSON}

            #add this feature to the total dictionary that will be used to make the file.
            geoJSONFile["features"].append(featureInGeoJSON)

        return geoJSONFile
