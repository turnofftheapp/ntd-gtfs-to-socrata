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
APP_TOKEN = {'X-APP-Token': 'FvuD9i0QMVotyBS8KxUOT5CvE'}
STANDARD_HEADERS = { 'Content-Type': 'application/json' }
UPLOAD_HEADERS = { 'Content-Type': 'text/csv' }
DOMAIN_URL = 'https://data.bts.gov'
AGENCY_FEED_DATASET_ID = "dw2s-2w2x" # Test dataset with 3 rows 
#AGENCY_FEED_DATASET_ID = "ymsd-c3s5" # Final dataset with all rows
CURRENT_CATALOG_LINK = "https://data.bts.gov/api/views/metadata/v1" # This is the link to all sets in the NTD catalog
CURRENT_CATALOG = json.loads(requests.get(CURRENT_CATALOG_LINK + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS).content)
ALL_STOP_LOCATIONS_DATASET_LINK = 'https://data.bts.gov/dataset/National-Transit-Map-All-Stop-Locations/39cr-5x89'
ALL_STOP_LOCATIONS_ENDPOINT = 'https://data.bts.gov/resource/39cr-5x89'
PRIVATE_DATASET_ENDPOINT = "https://data.bts.gov/resource/ngsm-beqg"

UPDATE_ACTION = 'update'
CREATE_ACTION = 'create'
BUS_UPSERT_ACTION  = 'bus stop upsert'
BUS_UPSERT_FAIL_ACTION = 'bus stops failed to upsert'
INVALID_URL_ACTION = 'record invalid url'
FEED_ID_PREFIX = "Feed ID: " # This is saved as part of the catalog entry description and allows identifying if a dataset for a given Agency Feed already exists in the Socrata catalog
TO_INVALID_RECORD = 'To invalid record' #This is the key to a dictionary which holds a boolean which labels a bus stops line in a stops.txt file as valid or not
OMIT_BUS_COLUMN_VALUE = 'omit'
KEEP_STOP = 'keep stop'
DELETE_STOP = 'delete stop'


# The below will be the change log that is emailed out once the script is finished running
INVALID_URLS = {}
BUS_STOPS_UPSERTED = {}
BUS_STOPS_NOT_UPSERTED = {}
DATA_CREATED = {}
DATA_UPDATED = {}
CHANGE_LOG = {"Data created" : DATA_CREATED, "Data updated" : DATA_UPDATED, "Bus stop upsertion attempts": BUS_STOPS_UPSERTED, "Invalid GTFS URLs": INVALID_URLS, "Unsuccessfull bus stop upserts": BUS_STOPS_NOT_UPSERTED}

# This function takes in a catalog entry and returns its "thumbprint" that can be used to update the changelog
def getCatalogThumbPrint(catalogRow):
  thumbPrint = {}
  thumbPrint['Name'] = catalogRow['name']
  thumbPrint['FeedID'] = getCatalogEntryFeedID(catalogRow['description'])
  thumbPrint['Fourfour'] = catalogRow['id']
  return thumbPrint

# This function takes in an agencyFeedRow and returns its "thumbprint" that can be used to update the changelog
def getAgencyFeedThumbPrint(agencyFeedRow):
  thumbPrint = {}
  thumbPrint['Name'] = agencyFeedRow['agency_name']
  thumbPrint['FeedID'] = agencyFeedRow['feed_id']
  thumbPrint['Fourfour'] = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])
  return thumbPrint

# Checks if the given URL is reachable and if so returns <GET request response object/data>, None
# If URL has issues return: None, <error message>
def urlIsValid(url):
  try:
    get = requests.get(url)
    if get.ok:
      return get, None
    else:
      errorMessage = f"{url}: is Not reachable, status_code: {get.status_code}"
  except Exception as e:
    errorMessage = getattr(e, 'message', repr(e))

  return None, errorMessage

# From: https://github.com/django/django/blob/stable/1.3.x/django/core/validators.py#L45
def urlIsValidStatic(url):
  urlRegex = re.compile(
    r'^(?:http|ftp)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
    r'localhost|' #localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)
  return (re.match(urlRegex, url) is not None)

def updateChangeLog(entryThumbPrint, action, Message='',url='',busNumbers={}):
  if action == CREATE_ACTION:
    DATA_CREATED[entryThumbPrint['FeedID']] = [
      entryThumbPrint['Name']
    ]
  elif action == UPDATE_ACTION:
    DATA_UPDATED[entryThumbPrint['FeedID']] = [
      entryThumbPrint['Name'],
      "https://data.bts.gov/d/" + entryThumbPrint['Fourfour']
    ]
  elif action == BUS_UPSERT_ACTION:
    BUS_STOPS_UPSERTED[entryThumbPrint['FeedID']] = [
      entryThumbPrint['Name'],
      Message,
      busNumbers
    ]
  elif action == BUS_UPSERT_FAIL_ACTION:
    BUS_STOPS_NOT_UPSERTED[entryThumbPrint['FeedID']] = [
      entryThumbPrint['Name'],
      Message,
      busNumbers
    ]
  elif action == INVALID_URL_ACTION:
    INVALID_URLS[entryThumbPrint['FeedID']] = [
      entryThumbPrint['Name'],
      "URL: " + url,
      Message
    ]


# Parses the GTFS zip file link out of the decodedMetadata
def getFetchLinkUrl(description):
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

# This function takes in a list and returns the same list with all items in it being cleared of leading and trailing whitespaces
def clearWhiteSpaces(listWithWhiteSpaceCharactersMaybe):
  newList = []
  for item in listWithWhiteSpaceCharactersMaybe:
    newitemWithoutQuotes = item.strip('"')
    newItem = newitemWithoutQuotes.strip()
    newList.append(newItem)
  return newList

def makeStopsObject(bytes):
  lineList = bytes.decode('UTF-8-sig').split("\n")
  headers = clearWhiteSpaces(lineList[0].split(","))
  stopsObject = {}
  for header in headers:
    stopsObject[header] = []
  i=0
  while i < len(lineList):
    j=0
    stopAsList = clearWhiteSpaces(lineList[i].split(","))
    if len(stopAsList) > 1: #last items in the list seemed to be empty and were throwing an error
      for header in headers:
        stopsObject[header].append(stopAsList[j])
        j+=1
    i += 1
  return stopsObject


# This function just gets rid of leading and trailing white spaces and quotes from a possible lat or lon
def stripNum(value):
  newValue = value.strip()
  allDoubleQuotesValue = newValue.replace("'",'"') #Now all quotes in value are double quotes
  return allDoubleQuotesValue.strip('"')

# This function validates that a latitude or lingitude are acutally latitudes and longitudes.
def validateCoordinates(lat,lon):
  try:
    numLat = float(lat)
    numLon = float(lon)
  except Exception as e:
    return False
  if numLat >= -90 and numLat <=90 and numLon >= -180 and numLon <= 180:
    return True
  else:
    return False
  
# This function validates that the locationType is actually a number instead of a string that cant be turned into a number
def validateLocationType(locationType):
  if locationType == OMIT_BUS_COLUMN_VALUE or locationType == '':
    return True
  try:
    numLocation = float(locationType)
  except Exception as e:
    return False
  return True


# This funciton takes in a line number of the stops.txt file, the feedID of where the stop file came from and the 
# stopsObject which was made of the data from the stops.txt file. The indecies in stops Object corespond with the line
# number in the stops.txt file. The purpose of this file is to further process the data in stopsObject, index by index
# to do a bulk upsert with a variable made of stops made with this function
def makeStopLine(stopFileLine,feedID,stopsObject):
  stopName = stopsObject['stop_name'][stopFileLine]
  stopLat = stripNum(stopsObject['stop_lat'][stopFileLine])
  stopLon = stripNum(stopsObject['stop_lon'][stopFileLine])
  try:
    locationType = stopsObject['location_type'][stopFileLine]
  except Exception as e:
    locationType = OMIT_BUS_COLUMN_VALUE
  try:
    stopID = stopsObject['stop_id'][stopFileLine]
  except Exception as e:
    stopID = OMIT_BUS_COLUMN_VALUE
  try:
    stopCode = stopsObject['stop_code'][stopFileLine]
  except Exception as e:
    stopCode = OMIT_BUS_COLUMN_VALUE
  try:
    stopZoneID = stopsObject['zone_id'][stopFileLine]
  except Exception as e:
    stopZoneID = OMIT_BUS_COLUMN_VALUE
  
  
  # The below if statment is to ensure the header line is built properly
  if(stopName == 'stop_name'):
    feedID = 'feed_id'
    stopLocation = 'stop_location'
  else: 
    # feed_id_stop_id is created outside this else loop because its only the feed_id that needs to be adjusted based on whether or not
    # the entry is the first one
    stopLocation = 'POINT('+stopLon+' '+stopLat+')'
  feed_id_stop_id = feedID + "_" + stopID
  
  stopDict = {}
  stopDict['line'] = ''
  possibleFields = [feed_id_stop_id,stopCode,stopName,stopID,stopLat,stopLon,stopZoneID,locationType,stopLocation]
  for field in possibleFields:
    if field != OMIT_BUS_COLUMN_VALUE:
      stopDict['line'] = stopDict['line'] + field + ","
  stopDict['line'] = stopDict['line'][:-1] + "\n" #removes the comma most recently added before putting the newline character

  # TODO make the below check more rigorous. Alot of upsertions are failing because some lat and longs are random english that was probably meant to be in some other field
  if stopFileLine != 0: # If stop==0, then it is the header row and wont need to go through these checks
    if not validateCoordinates(stopLat,stopLon):
      stopDict[TO_INVALID_RECORD] = True
    elif not validateLocationType(locationType):
      stopDict[TO_INVALID_RECORD] = True
    else:
      stopDict[TO_INVALID_RECORD] = False
  else:
    stopDict[TO_INVALID_RECORD] = False

  return stopDict


def locateDeletions(catalogRowThumbPrint, stopsObject):
  # Getting the catalogStop data from the ALL_STOP_LOCATIONS dataset
  relevantStopsEndpoint = ALL_STOP_LOCATIONS_ENDPOINT + ".json?$where=starts_with(feed_id_stop_id, '" + catalogRowThumbPrint['FeedID'] + "')"
  relevantStopsRequest = requests.get(relevantStopsEndpoint, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
  relevantStops = json.loads(relevantStopsRequest.content.decode('UTF-8'))
  catalogDict = {}
  for catalogStop in relevantStops:
    catalogDict[catalogStop['feed_id_stop_id']] = KEEP_STOP
  
  # Getting relevant identifiers from the one specific agency in the catalog that we are looking at
  incomingIdentifiers = []
  for stopID in stopsObject['stop_id']:
    if stopID == 'stop_id': # The catalogStops data has no header entry, so we will skip the header entry in the stopsObject which is derived from the catalogRow for that specific agency
      continue
    incomingIdentifiers.append(catalogRowThumbPrint['FeedID'] + "_" + stopID)

  toDelete = []
  # Any stop that is not in the stopsObject but in the query needs to he set for deletion
  for catalogIdentifier in catalogDict:
    if catalogIdentifier not in incomingIdentifiers:
      catalogDict[catalogIdentifier] = DELETE_STOP
      toDelete.append(
        {
        "feed_id_stop_id": catalogIdentifier,
        ":deleted" : True
        }
      )
  return toDelete

def deleteIfNecessary(catalogRowThumbPrint,stopsObject,requestResults):
  
  stopsToDelete = locateDeletions(catalogRowThumbPrint, stopsObject)
  if len(stopsToDelete) > 0:
    deleteCatalogEntryBusStopsRequest = requests.post(ALL_STOP_LOCATIONS_ENDPOINT, json.dumps(stopsToDelete), APP_TOKEN, headers=STANDARD_HEADERS, auth=CREDENTIALS)
    #requestResults = requestResults + "\n" + deleteCatalogEntryBusStopsRequest.content.decode('UTF-8').split("\n")[4].replace('"','')
    requestResults['Rows Deleted'] = int(deleteCatalogEntryBusStopsRequest.content.decode('UTF-8').split("\n")[4].split(":")[1])
  
  return requestResults

# This scans the current catalog, and for each entry, looks for busStop data, and if any stops are not already in the 
# busStopEntry in the catalog, that busStop is added
# updateTransitStopDataset() MUST be run AFTER updateCatalog() since this function scans the current catalog for updates to make to the bus stop data.
def updateTransitStopDataset():
  for catalogRow in CURRENT_CATALOG: 
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      catalogEntryFetchLink = getFetchLinkUrl(catalogRow['description'])

      # Skip invalid URLs or cases where GTFS URL was not found in dataset description
      if catalogEntryFetchLink != None and urlIsValidStatic(catalogEntryFetchLink):
        print("Upserting stop locations from " + catalogRow['name'])

        # The below zipRequest contains multiple files. The stops.txt file must be gotten out of the content of this request
        # then, the stops.txt file can be iterated through and stops from it can be added to the 'allCatalogBusStops' by upserting them
        catalogRowThumbPrint = getCatalogThumbPrint(catalogRow)
        try:
          zipRequest = requests.get(catalogEntryFetchLink)
          with open(os.getcwd()+"/tempzip.zip", "wb") as zip:
            zip.write(zipRequest.content)
          z = zipfile.ZipFile(os.getcwd()+"/tempzip.zip", "r")
          stopFile = z.read("stops.txt")
        except Exception as e:
          updateChangeLog(catalogRowThumbPrint, INVALID_URL_ACTION, Message=getattr(e, 'message', repr(e)),url=catalogEntryFetchLink)
          continue

        stopsObject = makeStopsObject(stopFile)
        existingFeedID = getCatalogEntryFeedID(catalogRow['description'])
        newStopData = ""
        lineCount = 0 # This includes the header!
        validLineCount = 0 # This includes the header!
        invalidLines = "" 
        while lineCount < len(stopsObject['stop_lat']):
          newStopLine = makeStopLine(lineCount,existingFeedID,stopsObject)
          lineCount += 1

          if lineCount == 1: # Adding header to the invalid lines data
            invalidLines = invalidLines + newStopLine['line']

          if newStopLine[TO_INVALID_RECORD] == False: #if it is a valid line
            newStopData = newStopData + newStopLine['line']
            validLineCount += 1 
          else:
            invalidLines = invalidLines + newStopLine['line']

        try:
          postCatalogEntryBusStopsRequest = requests.post(ALL_STOP_LOCATIONS_ENDPOINT, newStopData, APP_TOKEN, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
          requestResults = json.loads(postCatalogEntryBusStopsRequest.content.decode('UTF-8'))
        except Exception as e:
          try:
            postCatalogEntryBusStopsRequest = requests.post(ALL_STOP_LOCATIONS_ENDPOINT, newStopData.encode('utf-8'), APP_TOKEN, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
            requestResults = json.loads(postCatalogEntryBusStopsRequest.content.decode('UTF-8'))
          except Exception as e:
            print("Exception when upserting stop locations from " + catalogRow['name'] + ": " + str(e))
        
        os.remove(os.getcwd()+"/tempzip.zip")

        updatedRequestResults = deleteIfNecessary(catalogRowThumbPrint, stopsObject, requestResults)

        busLineDict = {}
        busLineDict['total stops.txt lines'] = lineCount - 1 # Minus 1 to account for the header
        busLineDict['valid lines'] = validLineCount -1 # Minus 1 to account for the header
        busLineDict['invalid lines'] = lineCount - validLineCount
        
        if postCatalogEntryBusStopsRequest.ok:
          updateChangeLog(catalogRowThumbPrint, BUS_UPSERT_ACTION, Message=updatedRequestResults, busNumbers=busLineDict)
        else:
          upsertErrorMsg = "Error upserting stop locations, status_code: " + str(postCatalogEntryBusStopsRequest.status_code)
          updateChangeLog(catalogRowThumbPrint, BUS_UPSERT_FAIL_ACTION, Message=upsertErrorMsg, busNumbers=busLineDict)

def getMetadataFieldIfExists(fieldName, agencyFeedRow):
  if fieldName in agencyFeedRow:
    return agencyFeedRow[fieldName]
  return ""

def setMetadata(agencyFeedRow, fetchLinkErrorMessage):
  gtfsUrlFieldValue = getMetadataFieldIfExists('fetch_link', agencyFeedRow)
  if gtfsUrlFieldValue == "":
    gtfsUrlFieldValue = "No URL provided"
  elif fetchLinkErrorMessage != None:
    gtfsUrlFieldValue = "URL (" + gtfsUrlFieldValue + ") is invalid: " + fetchLinkErrorMessage

  description = "Agency Name: " + agencyFeedRow['agency_name'] + "\n"
  description += "NTD Name: " + getMetadataFieldIfExists('ntd_name', agencyFeedRow) + "\n"
  description += "NTD ID: " + agencyFeedRow['ntd_id'] + "\n"
  description += FEED_ID_PREFIX + agencyFeedRow['feed_id'] + "\n"
  description += "GTFS: " + getMetadataFieldIfExists('has_gtfs', agencyFeedRow) + "\n"
  description += "GTFS URL: " + gtfsUrlFieldValue + "\n"
  description += "Agency URL: " + getMetadataFieldIfExists('agency_website', agencyFeedRow) + "\n"
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
  fetchLinkZipFileUrl = getMetadataFieldIfExists('fetch_link', agencyFeedRow)
  fetchLinkResponseIfValid,fetchLinkErrorMessage = urlIsValid(fetchLinkZipFileUrl)

  if fetchLinkZipFileUrl != "" and fetchLinkErrorMessage != None:
    updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), INVALID_URL_ACTION, Message=fetchLinkErrorMessage, url=fetchLinkZipFileUrl)
  
  ########
  ### Step 1a: Create new revision
  ### In this step you will want to put the metadata you'd like to update in JSON format along with the action you'd like to take This sample shows the default public metadata fields, but you can also update custom and private metadata here.
  ########
  revision_url = f'{DOMAIN_URL}/api/publishing/v1/revision'
  if fourfour == None:
    action_type = 'update' #Options are Update, Replace, or Delete
    url_for_step_1_post = revision_url
    print("Creating dataset for " + agencyFeedRow['agency_name'])
  else:
    action_type = 'replace'
    url_for_step_1_post = f'{revision_url}/{fourfour}'
    print("Updating dataset for " + agencyFeedRow['agency_name'] + " (" + url_for_step_1_post + ")")

  permission = 'private'
  metadata = setMetadata(agencyFeedRow, fetchLinkErrorMessage)
  body = json.dumps({
    'metadata': metadata,
      'action': {
        'type': action_type,
        'permission': permission
      }
  })
  update_revision_response = requests.post(url_for_step_1_post, data=body, headers=STANDARD_HEADERS, auth=CREDENTIALS)
    
  # Do not upload .ZIP file for this catalog record if the fetch_link was missing or response was invalid
  if fetchLinkResponseIfValid != None:
    ##########################
    ### Step 2: Create new source
    ##########################
    create_source_uri = update_revision_response.json()['links']['create_source']
    create_source_url = f'{DOMAIN_URL}{create_source_uri}'

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
    bytes = fetchLinkResponseIfValid.content
    upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
    upload_url = f'{DOMAIN_URL}{upload_uri}'
    upload_response = requests.post(upload_url, data=bytes, headers=UPLOAD_HEADERS, auth=CREDENTIALS)


  #########
  #Step 4: Apply revision (publishes changes)
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
# Returns a fourfour if it finds a matching FeedID, returns None if no matching FeedID is found
def getFourfourFromCatalogonMatchingFeedID(incoming_feed_id):
  for catalogRow in CURRENT_CATALOG:
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      if catalogRow['description'] == None: #this might be the issue
        existingFeedID = None # Otherwise, we get an error when running getCatalogEntryFeedID on the row
      else:
        existingFeedID = getCatalogEntryFeedID(catalogRow['description']) # Identify FeedID in catalogRow
      
      if existingFeedID == incoming_feed_id: 
        return catalogRow['id'] # This is a fourfour
  return None # This runs if no matching feed id is found in the entire catalog


# This is the highest level function that takes in the data, iterates through it, 
# checking the field for the fourfour and deciding whether or not to create or update
# each row of data
def updateCatalog():
  
  api_request = "https://data.bts.gov/resource/" + AGENCY_FEED_DATASET_ID + ".json"
  api_request += "?$where=have_consent_for_ntm=True" # Filter to only import feeds where original_consent_declined field is FALSE

  # agencyFeedResponse below is the incoming data that is being added to or changed in the NTDBTS catalog
  agencyFeedResponse = requests.get(api_request, headers=STANDARD_HEADERS, auth=CREDENTIALS)
  
  for agencyFeedRow in json.loads(agencyFeedResponse.content):
    agencyFeedRowFourfour = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])
    if agencyFeedRowFourfour == None:
      revision_response = revision(None, agencyFeedRow)
      if revision_response != None:
        updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), CREATE_ACTION)
    else:
      revision_response = revision(agencyFeedRowFourfour, agencyFeedRow)
      if revision_response != None:
        updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), UPDATE_ACTION)

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
    lineCount = 0 # This includes the header!
    validLineCount = 0 # This includes the header!
    invalidLines = "" 
    while lineCount < len(stopsObject['stop_lat']):
      newStopLine = makeStopLine(lineCount,existingFeedID,stopsObject)
      lineCount += 1

      if lineCount == 1: # Adding header to the invalid lines data
        invalidLines = invalidLines + newStopLine['line']

      if newStopLine[TO_INVALID_RECORD] == False: #if it is a valid line
        newStopData = newStopData + newStopLine['line']
        validLineCount += 1 
      else:
        invalidLines = invalidLines + newStopLine['line']
          
    postCatalogEntryBusStopsRequest = requests.put(ALL_STOP_LOCATIONS_ENDPOINT, newStopData, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
    requestResults = json.loads(postCatalogEntryBusStopsRequest.content.decode('UTF-8'))
        

def Main():
  #updateCatalog()
  updateTransitStopDataset()
  #resetTransitStopDataset() # Only uncomment this line when you want to clear out the stops entry for test purposes

  with open('CHANGE_LOG.txt', 'w') as f:
    f.write(json.dumps(CHANGE_LOG, indent=4))

Main()

