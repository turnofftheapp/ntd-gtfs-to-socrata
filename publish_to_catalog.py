import pdb
import requests
import os
import json
import re
from operator import itemgetter
from urllib.request import urlopen
from datetime import datetime
import zipfile
from zipfile import ZipFile
import csv


CREDENTIALS = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 
# I know it is bad practice to hard code passwords into a file, but since other people are going to
# be using this in the future, shouldn't we be including it in here so that every employee who uses 
# this doesn't need to add a new environment variable?
APP_TOKEN = {'X-APP-Token': 'FvuD9i0QMVotyBS8KxUOT5CvE'}
STANDARD_HEADERS = { 'Content-Type': 'application/json' }
UPLOAD_HEADERS = { 'Content-Type': 'text/csv' }
DOMAIN_URL = 'https://data.bts.gov'
AGENCY_FEED_DATASET_ID = "dw2s-2w2x" # Test dataset with 3 rows 
#AGENCY_FEED_DATASET_ID = "yj2k-sj77" # Final dataset with all rows
CURRENT_CATALOG_LINK = "https://data.bts.gov/api/views/metadata/v1" # This is the link to all sets in the NTD catalog
CURRENT_CATALOG = json.loads(requests.get(CURRENT_CATALOG_LINK + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS).content)
ALL_STOP_LOCATIONS_DATASET_LINK = 'https://data.bts.gov/dataset/National-Transit-Map-All-Stop-Locations/39cr-5x89'
ALL_STOP_LOCATIONS_ENDPOINT = 'https://data.bts.gov/resource/39cr-5x89'
UPDATE_ACTION = 'update'
CREATE_ACTION = 'create'

FEED_ID_PREFIX = "Feed ID: " # This is saved as part of the catalog entry description and allows identifying if a dataset for a given Agency Feed already exists in the Socrata catalog

# The below will be the change log that is emailed out once the script is finished running
INVALID_URLS = {}
BUS_STOPS_UPSERTED = {}
DATA_CREATED = {}
DATA_UPDATED = {}
CHANGE_LOG = {"Data created" : DATA_CREATED, "Data updated" : DATA_UPDATED, "Bus stop upsertion attempts": BUS_STOPS_UPSERTED, "Invalid GTFS URLs": INVALID_URLS}

# This funcitons checks if a gtfs link is reachable
def urlIsValid(url,agencyFeedRow):
  try:
    get = requests.get(url)
    if get.status_code == 200 or get.status_code == 201:
      print(f"{url}: is reachable")
      return True
    else:
      errorMessage = f"{url}: is Not reachable, status_code: {get.status_code}"
      print(errorMessage)
      updateInvalidUrlLog(agencyFeedRow,errorMessage)
      return False
      
  except requests.exceptions.RequestException as e:
    updateInvalidUrlLog(agencyFeedRow,e)
    return False


# This function updates the standard portion of the change log due to the data coming from an agencyFeedRow as opposed to a catalogRow
def updateChangeLog(agencyFeedRow, action, fourfour):
  name = agencyFeedRow['agency_name']
  feedID = agencyFeedRow['feed_id']
  dataLinkStart = 'https://data.bts.gov/d/'
  changelogValue = [name,f'{dataLinkStart}{fourfour}']
  if action == CREATE_ACTION:
    DATA_CREATED[feedID] = changelogValue
  elif action == UPDATE_ACTION:
    DATA_UPDATED[feedID] = changelogValue
  
# This function updates the GTFS INVALID_URL portion of the changelog only
def updateInvalidUrlLog(agencyFeedRow,errorMessage):
  fourfour = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])
  name = agencyFeedRow['agency_name']
  feedID = agencyFeedRow['feed_id']
  dataLinkStart = 'https://data.bts.gov/d/'
  changelogValue = [name,f'{dataLinkStart}{fourfour}',errorMessage]
  INVALID_URLS[feedID] = changelogValue

# Parses the GTFS zip file link out of the decodedMetadata
def getZipUrl(description):
  locateLogic = re.compile('\\nGTFS URL: .*\\nAgency URL:')
  locateResult = locateLogic.search(description)
  if locateResult == None:
    return None
  else:
    return locateResult.group().split(" ")[2].split("\n")[0]

# Locates the FeedID within the description field of catalogRow and returns it. Returns None if not found
def getCatalogEntryFeedID(catalogRowDescription):
    locateLogic = re.compile('[\n]' + FEED_ID_PREFIX + '.+[\n]') # Defines the regex logic to be ran on the description of catalogRow to look for the FeedID
    locateResult = locateLogic.search(catalogRowDescription) # Applys the logic above to the actual description
    if locateResult == None:
      return None
    else:
      locateResultList = locateResult.group().split(FEED_ID_PREFIX)
      feedID = locateResultList[1][:len(locateResultList[1]) -1]
      return feedID

def makeStopsObject(bytes):
  lineList = bytes.decode('UTF-8').split("\n")
  headers = lineList[0].split(",")
  stopsObject = {}
  for header in headers:
    stopsObject[header] = []
  i=0
  while i < len(lineList):
    j=0
    stopAsList = lineList[i].split(",")
    if len(stopAsList) > 1: #last items in the list seemed to be empty and were throwing an error
      for header in headers:
        stopsObject[header].append(stopAsList[j])
        j+=1
    i += 1
  return stopsObject


# This funciton takes in a line from the stop.txt file within the GTFS zip file and
# returns it in the format needed to do a bulk upsert with a variable made of stops made with this function

# This funciton takes in an integer and the feedID of where the stop file came from and
# returns a stops data in the format needed to do a bulk upsert with a variable made of stops made with this function
def makeStopLine(stop,feedID,stopsObject):
  stopID = stopsObject['stop_id'][stop]
  stopName = stopsObject['stop_name'][stop]
  stopLat = stopsObject['stop_lat'][stop]
  stopLon = stopsObject['stop_lon'][stop]
  stopCode = stopsObject['stop_code'][stop]
  stopZoneID = stopsObject['zone_id'][stop]
  locationType = stopsObject['location_type'][stop]

  # The below if statment is to ensure the header line is built properly
  if(stopName == 'stop_name'):
    feedID = 'feed_id'
    stopLocation = 'stop_location'
  else: 
    # feed_id_stop_id is created outside this else loop because its only the feed_id that needs to be adjusted based on whether or not
    # the entry is the first one
    stopLocation = 'POINT('+stopLon+' '+stopLat+')'
  feed_id_stop_id = feedID + "_" + stopID
  
  stopUpsertLine = feed_id_stop_id + ',' + stopID +',' + stopCode +',' + stopName +',' + stopID + ',' + stopLat + ',' + stopLon + ',' + stopZoneID +',' + locationType +',' + stopLocation +"\n"
  return stopUpsertLine




# This scans the current catalog, and for each entry, looks for busStop data, and if any stops are not already in the 
# busStopEntry in the catalog, that busStop is added
# updateTransitStopDataset() MUST be run AFTER updateCatalog() since this function scans the current catalog for updates to make to
# the bus stop data.
def updateTransitStopDataset():
  print("upserting stops was called")
  # The below for loop iterates through the existing catalog, identifying entrys that we deal with in order to get their bus stop data
  # and add that data to the catalog bus stop data
  for catalogRow in CURRENT_CATALOG: 
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      catalogEntryZip = getZipUrl(catalogRow['description'])
      if catalogEntryZip != None: #needed this if statement because some agencies were starting to use the "national transit map" tag
        print("zip for")
        print(catalogRow['name'])
        print(catalogEntryZip)
        # The below zipRequest contains multiple files. The stops.txt file must be gotten out of the content of this request
        # then, the stops.txt file can be iterated through and stops from it can be added to the 'allCatalogBusStops' by upserting them
        zipRequest = requests.get(catalogEntryZip)
        
        with open(os.getcwd()+"/tempzip.zip", "wb") as zip:
          zip.write(zipRequest.content)
        z = zipfile.ZipFile(os.getcwd()+"/tempzip.zip", "r")
        try:
          stopFile = z.read("stops.txt")
          
        except:
          print("no stops file in " + catalogRow["name"])
          continue
        stopsObject = makeStopsObject(stopFile)
        existingFeedID = getCatalogEntryFeedID(catalogRow['description'])
        newStopData = ""
        count = 0
        while count < len(stopsObject['stop_id']):
          newStopLine = makeStopLine(count,existingFeedID,stopsObject)
          newStopData = newStopData + newStopLine
          count += 1
        postCatalogEntryBusStopsRequest = requests.post(ALL_STOP_LOCATIONS_ENDPOINT, newStopData, APP_TOKEN, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
        requestResults = json.loads(postCatalogEntryBusStopsRequest.content.decode('UTF-8'))
        
        os.remove(os.getcwd()+"/tempzip.zip")
      
        # The below determines how to update the busStop portion of the change log based on the status of postCatalogEntryBusStopsRequest
        strCount = str(count)
        print("script found "+strCount+" stops")
        if not postCatalogEntryBusStopsRequest.ok:
          print("Error upserting bus stops")
          requestResults = 'There was an error upserting stops from this catalog entry. There were 0 upsertions from this entry.'
        
        # @TODO: record a log entry for bus stops that includes total number of lines in the stops.txt file plus the total number of rows updated or created from requestResults. These numbers should be equal but it will be good to see if they are not in order to investigate potential data issues.
        #Funciton was replaced: updateBusChangeLog(catalogRow,requestResults)
          

def getMetadataFieldIfExists(fieldName, agencyFeedRow):
  if fieldName in agencyFeedRow:
    return agencyFeedRow[fieldName]
  return ""

def getMetadataUrlFieldIfExists(fieldName, agencyFeedRow):
  if fieldName not in agencyFeedRow:
    updateInvalidUrlLog(agencyFeedRow, fieldName + ": Field does not exist")
    return ""

  # Validate URL (from https://github.com/django/django/blob/stable/1.3.x/django/core/validators.py#L45)
  # urlRegex = re.compile(
  #    r'^(?:http|ftp)s?://' # http:// or https://
  #    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
  #    r'localhost|' #localhost...
  #    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
  #    r'(?::\d+)?' # optional port
  #    r'(?:/?|[/?]\S+)$', re.IGNORECASE)#

  #  if (re.match(urlRegex, agencyFeedRow[fieldName]) is None):
  #    updateInvalidUrlLog(agencyFeedRow, fieldName + ": URL is invalid.")
  #    return ""

  return agencyFeedRow[fieldName]


def setMetadata(agencyFeedRow):
  description = "Agency Name: " + agencyFeedRow['agency_name'] + "\n"
  description += "NTD Name: " + getMetadataFieldIfExists('ntd_name', agencyFeedRow) + "\n"
  description += "NTD ID: " + agencyFeedRow['ntd_id'] + "\n"
  description += FEED_ID_PREFIX + agencyFeedRow['feed_id'] + "\n"
  description += "GTFS: " + getMetadataFieldIfExists('has_gtfs', agencyFeedRow) + "\n"
  description += "GTFS URL: " + getMetadataUrlFieldIfExists('fetch_link', agencyFeedRow) + "\n"
  description += "Agency URL: " + getMetadataUrlFieldIfExists('agency_website', agencyFeedRow) + "\n"
  description += "Region: " + getMetadataFieldIfExists('uza', agencyFeedRow) + "\n"
  description += "City: " + getMetadataFieldIfExists('city', agencyFeedRow) + "\n" # Update
  description += "State: " + getMetadataFieldIfExists('state', agencyFeedRow) + "\n" # Update
  
  # @TODO: set all required metadata
  return { 
    'name': "NTM: " + agencyFeedRow['agency_name'],
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


# 'fourfour' is the dataset ID of an existing dataset to update/replace
#the parameter variable 'set' is one row in the dataset that represents a "source" of data from some city somewhere
def revision(fourfour, agencyFeedRow):
  fetchLinkZipFileUrl = getMetadataUrlFieldIfExists('fetch_link', agencyFeedRow)
  
  # Skip uploading to catalog if ZIP file is not valid
  if not urlIsValid(fetchLinkZipFileUrl, agencyFeedRow): # This reports out on invalid GTFS urls
    return None

  print("revision was called")
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

  #@TODO: use the .get response from isValidZip() function for efficiency
  resp = requests.get(url=fetchLinkZipFileUrl)
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
  return apply_revision_response


# Takes in a row of incoming dataset metadata and iterates through the current catalog, looking for a matching feedID 
# in the catalog entries descriptions.
# Returns a fourfour if it finds a matching FeedID, returns null if no matching FeedID is found
def getFourfourFromCatalogonMatchingFeedID(incoming_feed_id):
  for catalogRow in CURRENT_CATALOG:
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      if catalogRow['description'] == None: #this might be the issue
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
        if agencyFeedRowFourfour == None:
          print("creating")
          # Since the revision is created below before the update to the changelog, the fourfour should exist
          # by the time the change log entry is entered for that new data
          revision_response = revision(None, agencyFeedRow)
          updateChangeLog(agencyFeedRow,CREATE_ACTION,None)
          
        else:
          print("replacing") 
          revision_response = revision(agencyFeedRowFourfour, agencyFeedRow)
          updateChangeLog(agencyFeedRow,UPDATE_ACTION,agencyFeedRowFourfour)


# This function can be run by itsself in order to clear out the busstop data from the bus stop entry in the socrata database
def resetTransitStopDataset():  
  with open(os.getcwd()+"/stopsStarter.txt", 'r') as file:
    stopFile = csv.reader(file)
    stopslist = [] #list of lists
    for line in stopFile:
      stopslist.append(line)
    headers = stopslist[0]
    stopsObject = {}
    for header in headers:
      stopsObject[header] = []
    i=0
    while i < len(stopslist):
      j=0
      stopAsList = stopslist[i]
      if len(stopAsList) > 1: #last items in the list seemed to be empty and were throwing an error
        for header in headers:
          stopsObject[header].append(stopAsList[j])
          j+=1
      i += 1
    existingFeedID =  '00009'
    newStopData = ""
    count = 0
    
    while count < len(stopsObject['stop_id']):
      newStopLine = makeStopLine(count,existingFeedID,stopsObject)
      count += 1
      newStopData = newStopData + newStopLine
    postCatalogEntryBusStopsRequest = requests.put(ALL_STOP_LOCATIONS_ENDPOINT, newStopData, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
    requestResults = json.loads(postCatalogEntryBusStopsRequest.content.decode('UTF-8'))
        

def Main():
  updateCatalog()
  updateTransitStopDataset()
  #resetTransitStopDataset() # Only uncomment this line when you want to clear out the stops entry in socrata
  
  with open('CHANGE_LOG.txt', 'w') as f:
    f.write(json.dumps(CHANGE_LOG, indent=4))

Main()

