#from _typeshed import NoneType
import pdb
import requests
import os
import json
import re
from operator import itemgetter
from urllib.request import urlopen
from datetime import datetime
import re
import zipfile
from zipfile import ZipFile


CREDENTIALS = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 
# I know it is bad practice to hard code passwords into a file, but since other people are going to
# be using this in the future, shouldn't we be including it in here so that every employee who uses 
# this doesn't need to add a new environment variable?
APP_TOKEN = {'X-APP-Token': 'FvuD9i0QMVotyBS8KxUOT5CvE'}
STANDARD_HEADERS = { 'Content-Type': 'application/json' }
UPLOAD_HEADERS = { 'Content-Type': 'text/csv' }
DOMAIN_URL = 'https://data.bts.gov'
AGENCY_FEED_DATASET_ID = "dw2s-2w2x"
CURRENT_CATALOG_LINK = "https://data.bts.gov/api/views/metadata/v1" # This is the link to all sets in the NTD catalog
CURRENT_CATALOG = json.loads(requests.get(CURRENT_CATALOG_LINK + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS).content)
ALL_STOP_LOCATIONS_DATASET_LINK = 'https://data.bts.gov/dataset/National-Transit-Map-All-Stop-Locations/39cr-5x89'
ALL_STOP_LOCATIONS_ENDPOINT = 'https://data.bts.gov/resource/39cr-5x89'
UPDATE_ACTION = 'update'
CREATE_ACTION = 'create'
BUS_UPSERT_ACTION  = 'bus stop upsert'



# The below will be the change log that is emailed out once the script is finished running
BUS_STOPS_UPSERTED = {}
DATA_CREATED = {}
DATA_UPDATED = {}
CHANGE_LOG = {"Data created" : DATA_CREATED, "Data updated" : DATA_UPDATED, "Bus stop upsertion attempts": BUS_STOPS_UPSERTED}


# This function updates the standard portion of the change log due to the data coming from an agencyFeedRow as opposed to a catalogRow
def updateChangeLog(agencyFeedRow, action):
  fourfour = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])
  name = agencyFeedRow['ntd_name']
  feedID = agencyFeedRow['feed_id']
  dataLinkStart = 'https://data.bts.gov/d/'
  changelogValue = [name,f'{dataLinkStart}{fourfour}']
  if action == CREATE_ACTION:
    DATA_CREATED[feedID] = changelogValue
  elif action == UPDATE_ACTION:
    DATA_UPDATED[feedID] = changelogValue
  
# This function updates the bus stop portion of the changelog only due to the data coming from a catalogRow as opposed to an agencyFeedRow
def updateBusChangeLog(catalogRow, countMessage):
  fourfour = catalogRow['id']
  name = catalogRow['name']
  feedID = getCatalogEntryFeedID(catalogRow['description'])
  dataLinkStart = 'https://data.bts.gov/d/'
  changelogValue = [name,f'{dataLinkStart}{fourfour}',countMessage]
  BUS_STOPS_UPSERTED[feedID] = changelogValue

# Parses the GTFS zip file link out of the decodedMetadata
def getZipUrl(description):
  locateLogic = re.compile('\\nGTFS URL: .*\.zip\\n')
  locateResult = locateLogic.search(description)
  if locateResult == None:
    return None
  else:
    locateResultList = locateResult.group().split(" ")
    return locateResultList[2].strip()

# Locates the FeedID within the description field of catalogRow and returns it. Returns None if not found
def getCatalogEntryFeedID(catalogRowDescription):
    locateLogic = re.compile('[\n]Feed ID: [0-9]+[\n]') # Defines the regex logic to be ran on the description of catalogRow to look for the FeedID
    locateResult = locateLogic.search(catalogRowDescription) # Applys the logic above to the actual description
    if locateResult == None:
      return None
    else:
      locateResultList = locateResult.group().split(" ")
      feedID = locateResultList[2][:len(locateResultList[2]) -1]
      return feedID

# This funciton takes in a line from the stop.txt file within the GTFS zip file and
# returns it in the format needed to do a bulk upsert with a variable made of stops made with this function
def makeStopLine(stop,feedID):
  stopList = stop.split(",")
  stopID = stopList[0]
  stopName = stopList[1]
  stopLat = stopList[2]
  stopLon = stopList[3]
  stopCode = stopList[4]
  stopZoneID = stopList[5]

  # The below if statment is to ensure the header line is built properly
  if(stopName == 'stop_name'):
    feedID = 'feed_id'
    locationType = 'location_type'
    stopLocation = 'stop_location'
  else: 
    # feed_id_stop_id is created outside this else loop because its only the feed_id that needs to be adjusted based on whether or not
    # the entry is the first one
    stopLocation = 'POINT('+stopLon+' '+stopLat+')'
    locationType = '0' #this one Ill definitely have to check up on
  feed_id_stop_id = feedID + "_" + stopID
  
  stopUpsertLine = feed_id_stop_id + ',' + stopID +',' + stopCode +',' + stopName +',' + stopID + ',' + stopLat + ',' + stopLon + ',' + stopZoneID +',' + locationType +',' + stopLocation +"\n"
  return stopUpsertLine


# This scans the current catalog, and for each entry, looks for busStop data, and if any stops are not already in the 
# busStopEntry in the catalog, that busStop is added
# updateTransitStopDataset() MUST be run AFTER updateCatalog() since this function scans the current catalog for updates to make to
# the bus stop data.
def updateTransitStopDataset():
  # The below for loop iterates through the existing catalog, identifying entrys that we deal with in order to get their bus stop data
  # and add that data to the catalog bus stop data
  for catalogRow in CURRENT_CATALOG: 
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      catalogEntryZip = getZipUrl(catalogRow['description'])
      print("zip = " + catalogEntryZip)
      
      
      
      
      # The below zipRequest contains multiple files. The stops.txt file must be gotten out of the content of this request
      # then, the stops.txt file can be iterated through and stops from it can be added to the 'allCatalogBusStops' by upserting them
      zipRequest = requests.get(catalogEntryZip)
      
      with open(os.getcwd()+"/tempzip.zip", "wb") as zip:
        zip.write(zipRequest.content)
      z = zipfile.ZipFile(os.getcwd()+"/tempzip.zip", "r")

      for filename in z.namelist():
        if filename == "stops.txt":
          stopFile = z.read(filename)
          print("stopFile")
          stringStops = stopFile.decode('UTF-8').split("\n")
          existingFeedID = getCatalogEntryFeedID(catalogRow['description'])
          newStopData = ""
          count = 0

          for stop in stringStops:
            if (stop != ""):
              newStopLine = makeStopLine(stop,existingFeedID)
              count += 1
              newStopData = newStopData + newStopLine
          postCatalogEntryBusStopsRequest = requests.post(ALL_STOP_LOCATIONS_ENDPOINT, newStopData, APP_TOKEN, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
          os.remove(os.getcwd()+"/tempzip.zip")
         
          # The below determines how to update the busStop portion of the change log based on the status of 
          strCount = str(count)
          if not postCatalogEntryBusStopsRequest.ok:
            print("Error upserting bus stops")
            countMessage = 'There was an error upserting stops from this catalog entry. There were 0 upsertions from this entry.'
          else:
            print('There were ' + strCount + ' stops upserted')
            countMessage = 'There were ' + strCount + ' stops upserted from this catalog entry'
          updateBusChangeLog(catalogRow, countMessage)
          




def getMetadataFieldIfExists(fieldName, agencyFeedRow):
  if agencyFeedRow[fieldName]:
    return agencyFeedRow[fieldName]
  return ""

def getMetadataUrlFieldIfExists(fieldName, agencyFeedRow):
  if agencyFeedRow[fieldName]:
    if agencyFeedRow[fieldName]["url"]:
      return agencyFeedRow[fieldName]["url"]
  return ""

def setMetadata(agencyFeedRow):
  description = "Agency Name: " + agencyFeedRow['agency_name'] + "\n"
  description += "NTD ID: " + agencyFeedRow['ntd_id'] + "\n"
  description += "Feed ID: " + agencyFeedRow['feed_id'] + "\n"
  description += "GTFS: " + getMetadataFieldIfExists('has_gtfs', agencyFeedRow) + "\n"
  description += "GTFS URL: " + getMetadataUrlFieldIfExists('fetch_link', agencyFeedRow) + "\n"
  description += "Agency URL: " + getMetadataUrlFieldIfExists('agency_website', agencyFeedRow) + "\n"
  description += "Region: " + getMetadataFieldIfExists('uza', agencyFeedRow) + "\n"
  description += "City: " + getMetadataFieldIfExists('city', agencyFeedRow) + "\n" # Update
  description += "State: " + getMetadataFieldIfExists('state', agencyFeedRow) + "\n" # Update
  
  # @TODO: set all required metadata
  return { 
    'name': "NTM: " + agencyFeedRow['ntd_name'],
    'description': description,
    "metadata" : {
      "custom_fields" : {
        "Common Core" : {
          "Contact Email" : "NationalTransitMap@dot.gov",
          "Contact Name" : "Derald Dudley",
          "License" : "https://creativecommons.org/licenses/by/4.0/",
          "Program Code" : "021:000",
          "Publisher" : "Bureau of Transportation Statistics",
          "Bureau Code" : "021:00",
          "Public Access Level" : "public"
        }
      }
    },
    'tags': ["national transit map"]
  }

# Takes in a row of incoming dataset metadata and iterates through the current catalog, looking for a matching feedID 
# in the catalog entries descriptions.
# Returns a fourfour if it finds a matching FeedID, returns null if no matching FeedID is found
def getFourfourFromCatalogonMatchingFeedID(incoming_feed_id):
  for catalogRow in CURRENT_CATALOG:
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      if catalogRow['description'] == None:
        existingFeedID = None # Otherwise, we get an error when running getCatalogEntryFeedID on the row
        #print("existingFeedID No desc")
        #print(existingFeedID)
      else:
        existingFeedID = getCatalogEntryFeedID(catalogRow['description']) # Identify FeedID in catalogRow
        #print("existingFeedID desc")
        #print(existingFeedID)
      
      if existingFeedID == incoming_feed_id: 
        #print("################################# catalogRow['id']: "+catalogRow['id'])
        return catalogRow['id'] # This is a fourfour

  return None


# 'fourfour' is the dataset ID of an existing dataset to update/replace
#the parameter variable 'set' is one row in the dataset that represents a "source" of data from some city somewhere
def revision(fourfour, agencyFeedRow):
  print(fourfour)
  ########
  ### Step 1a: Create new revisionIn this step you will want to put the metadata you'd like to update in JSON format along with the action you'd like to take This sample shows the default public metadata fields, but you can also update custom and private metadata here.
  ########
  revision_url = f'{DOMAIN_URL}/api/publishing/v1/revision'
  if fourfour == None:
    action_type = 'update' #Options are Update, Replace, or Delete
    url_for_step_1_post = revision_url
  else:
    action_type = 'replace'
    url_for_step_1_post = f'{revision_url}/{fourfour}'
  
  permission = 'private'
  metadata = setMetadata(agencyFeedRow)
  body = json.dumps({
    'metadata': metadata,
      'action': {
        'type': action_type,
        'permission': permission
      }
  })
  update_revision_response = requests.post(url_for_step_1_post, data=body, headers=STANDARD_HEADERS, auth=CREDENTIALS)
  if fourfour == None:
    fourfour = update_revision_response.json()['resource']['fourfour'] # Creating a new revision will return the 4x4 for your new dataset
    
  create_source_uri = update_revision_response.json()['links']['create_source'] # It will also return the URL you need to create a source 
  create_source_url = f'{DOMAIN_URL}{create_source_uri}'
  
  ##########################
  ### Step 2: Create new source
  ##########################
  now = datetime.now().strftime("%Y-%m-%d")
  filename = agencyFeedRow['ntd_id'] + " " + now + '.zip' 
  revision_source_type = 'upload'

  parse_source = False

  source_json = json.dumps({
    'source_type': {
      'type': revision_source_type,
      'filename': filename
    },
    'parse_options': {
      'parse_source': parse_source
    }
  })
  
  source_response = requests.post(create_source_url, data=source_json, headers=STANDARD_HEADERS, auth=CREDENTIALS)

  ##########################
  ### Step 3: Upload File to source_type
  ##########################
  resp = requests.get(url=getMetadataUrlFieldIfExists('fetch_link', agencyFeedRow))
  bytes = resp.content
  
  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{DOMAIN_URL}{upload_uri}'
  upload_response = requests.post(upload_url, data=bytes, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
 
  #########
  #Step 2a(5): Apply revisionHere you just apply your revision as you would if you were updating data.
  #########
  apply_revision_uri = update_revision_response.json()['links']['apply']
  apply_revision_url = f'{DOMAIN_URL}{apply_revision_uri}'
  revision_number = update_revision_response.json()['resource']['revision_seq']
  body = json.dumps({
  'resource': {
      'id': revision_number
    }
  })
  apply_revision_response = requests.put(apply_revision_url, data=body, headers=STANDARD_HEADERS, auth=CREDENTIALS)
  #return apply_revision_response



# This is the highest level function that takes in the data, iterates through it, 
# checking the field for the fourfour and deciding whether or not to create or update
# each row of data
def updateCatalog():
  # agencyFeedResponse below is the incoming data that is being added to or changed in the NTDBTS catalog
  agencyFeedResponse = requests.get("https://data.bts.gov/resource/" + AGENCY_FEED_DATASET_ID + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS)
  
  for agencyFeedRow in json.loads(agencyFeedResponse.content):
    # Only import feeds where original_consent_declined field is FALSE
  
    if 'original_consent_declined' in agencyFeedRow:
      if agencyFeedRow['original_consent_declined'] == False:
        
        # The line below calls the function that looks through metadata to determine if dataset exists 
        # and returns the given fourfour or keyword "None", based on what is returned, the decision to 
        # create or replace is made for that row of incoming data
        agencyFeedRowFourfour = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])

        name = agencyFeedRow['ntd_name']
        feedID = agencyFeedRow['feed_id']
        dataLinkStart = 'https://data.bts.gov/d/'
        
        # TODO find the fourfour of the newly created sets 
        # TODO change the condition of the if & elif statements below since we will have a fourfour either way
        fourfour = agencyFeedRowFourfour 

        changelogValue = [name,f'{dataLinkStart}{fourfour}'] #maybe consider .format
        if agencyFeedRowFourfour == None:
          print("creating")
          # Since the revision is created below before the update to the changelog, the fourfour should exist
          # by the time the change log entry is entered for that new data
          revision(None, agencyFeedRow)
          updateChangeLog(agencyFeedRow,CREATE_ACTION)
          
        else:
          print("replacing") 
          revision(agencyFeedRowFourfour, agencyFeedRow)
          updateChangeLog(agencyFeedRow,UPDATE_ACTION)

        

def Main():
  #updateCatalog()
  updateTransitStopDataset()
  print(CHANGE_LOG)

Main()

