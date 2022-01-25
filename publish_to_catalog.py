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
BUS_UPSERT_ACTION  = 'bus stop upsert'
INVALID_URL_ACTION = 'record invalid url'
FEED_ID_PREFIX = "Feed ID: " # This is saved as part of the catalog entry description and allows identifying if a dataset for a given Agency Feed already exists in the Socrata catalog
TO_INVALID_RECORD = 'To invalid record' #This is the key to a dictionary which holds a boolean which labels a bus stops line in a stops.txt file as valid or not
OMIT_BUS_COLUMN_VALUE = 'omit'

# The below will be the change log that is emailed out once the script is finished running
INVALID_URLS = {}
BUS_STOPS_UPSERTED = {}
DATA_CREATED = {}
DATA_UPDATED = {}
CHANGE_LOG = {"Data created" : DATA_CREATED, "Data updated" : DATA_UPDATED, "Bus stop upsertion attempts": BUS_STOPS_UPSERTED, "Invalid GTFS URLs": INVALID_URLS}

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
  thumbPrint['Name'] = agencyFeedRow['ntd_name']
  thumbPrint['FeedID'] = agencyFeedRow['feed_id']
  thumbPrint['Fourfour'] = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])
  return thumbPrint

# This funcitons checks if a gtfs link is reachable
def urlIsValid(url,agencyFeedRow):
  print("trying " + getAgencyFeedThumbPrint(agencyFeedRow)['Name'])
  try:
    get = requests.get(url)
    if get.status_code == 200 or get.status_code == 201:
      print(f"{url}: is reachable")
      print("success " + getAgencyFeedThumbPrint(agencyFeedRow)['Name'])
      return get
    else:
      errorMessage = f"{url}: is Not reachable, status_code: {get.status_code}"
      print(errorMessage)
      #updateInvalidUrlLog(agencyFeedRow,url,errorMessage)
      updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), INVALID_URL_ACTION, Message=errorMessage,url=url)
      print("Else on " + getAgencyFeedThumbPrint(agencyFeedRow)['Name'])
      return None
      
  except Exception as e:
    #updateInvalidUrlLog(agencyFeedRow,url,getattr(e, 'message', repr(e)))
    print("with agencyFeedRow")
    updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), INVALID_URL_ACTION, Message=getattr(e, 'message', repr(e)),url=url)
    print("Exception on  " + getAgencyFeedThumbPrint(agencyFeedRow)['Name'])
    return None

'''
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
  '''

def updateChangeLog(entryThumbPrint, action, Message='',url='',busNumbers={}):
  if entryThumbPrint['Fourfour'] == None:
    dataLink = "No data link"
  else:
    dataLink = 'https://data.bts.gov/d/' + entryThumbPrint['Fourfour']
  changelogValue = [entryThumbPrint['Name'],dataLink]
  if action == CREATE_ACTION:
    DATA_CREATED[entryThumbPrint['FeedID']] = changelogValue
  elif action == UPDATE_ACTION:
    DATA_UPDATED[entryThumbPrint['FeedID']] = changelogValue
  elif action == BUS_UPSERT_ACTION:
    feedID_name = entryThumbPrint['FeedID'] + "_" + entryThumbPrint['Name']
    changelogValue = [entryThumbPrint['Name'],dataLink,Message,busNumbers]
    BUS_STOPS_UPSERTED[feedID_name] = changelogValue
  elif action == INVALID_URL_ACTION:
    changelogValue = [
      entryThumbPrint['Name'],
      dataLink,
      "URL: " + url,
      Message
    ]
    INVALID_URLS[entryThumbPrint['FeedID']] = changelogValue


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

# This function takes in a list and returns the same list with all items in it being cleared of leading and trailing whitespaces
def clearWhiteSpaces(listWithWhiteSpaceCharactersMaybe,header=False):
  #if header:
    #pdb.set_trace()
  newList = []
  for item in listWithWhiteSpaceCharactersMaybe:
    #list1=[]
    #list1[:0]=item
    #for thing in list1:
        #if thing.isspace() and thing != " " and header:
            #pdb.set_trace()
    newitemWithoutQuotes = item.strip('"')
    newItem = newitemWithoutQuotes.strip()
    newList.append(newItem)
  return newList

def makeStopsObject(bytes):
  lineList = bytes.decode('UTF-8-sig').split("\n")
  headers = clearWhiteSpaces(lineList[0].split(","),True)
  #pdb.set_trace()
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
    print(e)
    return False
  if numLat >= -90 and numLat <=90 and numLon >= -180 and numLon <= 180:
    return True
  else:
    print("lat or lon were out of range")
    return False
  
# This function validates that the locationType is actually a number instead of a string that cant be turned into a number
def validateLocationType(locationType):
  if locationType == OMIT_BUS_COLUMN_VALUE or locationType == '':
    return True
  try:
    numLocation = float(locationType)
  except Exception as e:
    print(e)
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

  
  #stopDict['line'] = feed_id_stop_id +',' + stopCode +',' + stopName +',' + stopID + ',' + stopLat + ',' + stopLon + ',' + stopZoneID +',' + locationType +',' + stopLocation +"\n"
  
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




# This scans the current catalog, and for each entry, looks for busStop data, and if any stops are not already in the 
# busStopEntry in the catalog, that busStop is added
# updateTransitStopDataset() MUST be run AFTER updateCatalog() since this function scans the current catalog for updates to make to
# the bus stop data.
def updateTransitStopDataset():
  print("upserting stops was called")
  # The below for loop iterates through the existing catalog, identifying entrys that we deal with in order to get their bus stop data
  # and add that data to the catalog bus stop data
  
  '''
  notThere = True
  counter = 0
  '''
  for catalogRow in CURRENT_CATALOG: 
    '''
    counter+=1
    print(counter)
    if catalogRow['name'] == 'NTM: Massachusetts Bay Transportation Authority':
      notThere = False
    if notThere:
      continue
    '''
    #if catalogRow['name'] == 'NTM: Fairbanks North Star Borough':
    if catalogRow['name'] == "NTM: TEST: Pierce County Transportation Benefit Area Authority" or catalogRow['name'] == "TEST: Confederated Tribes of the Colville Indian Reservation" or catalogRow['name'] == "NTM: TEST: City of Yakima, dba: Yakima Transit":
      print(catalogRow['name'])
    #if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      catalogEntryZip = getZipUrl(catalogRow['description'])
      if catalogEntryZip != None: #needed this if statement because some agencies were starting to use the "national transit map" tag
        print(catalogRow['name'])
        # The below zipRequest contains multiple files. The stops.txt file must be gotten out of the content of this request
        # then, the stops.txt file can be iterated through and stops from it can be added to the 'allCatalogBusStops' by upserting them
        try:
          zipRequest = requests.get(catalogEntryZip)
          with open(os.getcwd()+"/tempzip.zip", "wb") as zip:
            zip.write(zipRequest.content)
          z = zipfile.ZipFile(os.getcwd()+"/tempzip.zip", "r")
          stopFile = z.read("stops.txt")
        except Exception as e:
          #updateInvalidUrlLog(catalogRow,catalogEntryZip, getattr(e, 'message', repr(e)))
          updateChangeLog(getCatalogThumbPrint(catalogRow), INVALID_URL_ACTION, Message=getattr(e, 'message', repr(e)),url=catalogEntryZip)
          print(getattr(e, 'message', repr(e)))
          #os.remove(os.getcwd()+"/tempzip.zip") # If aborting this iteration, we will get rid of the zip file locally
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
            print("trying to encode newStopData")
            postCatalogEntryBusStopsRequest = requests.post(ALL_STOP_LOCATIONS_ENDPOINT, newStopData.encode('utf-8'), APP_TOKEN, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
            requestResults = json.loads(postCatalogEntryBusStopsRequest.content.decode('UTF-8'))
          except Exception as e:
            pdb.set_trace()
            print(e)
        
        os.remove(os.getcwd()+"/tempzip.zip")
        busLineDict = {}
        busLineDict['total stops.txt lines'] = lineCount - 1 # Minus 1 to account for the header
        busLineDict['valid lines'] = validLineCount -1 # Minus 1 to account for the header
        busLineDict['invalid lines'] = lineCount - validLineCount
        # The below determines how to update the busStop portion of the change log based on the status of postCatalogEntryBusStopsRequest
        strLineCount = str(lineCount)
        strValidLineCount = str(validLineCount)

        print("script found "+strLineCount+" lines which included " + strValidLineCount + " valid lines.")
        if not postCatalogEntryBusStopsRequest.ok:
          print("Error upserting bus stops")
          pdb.set_trace()
          requestResults = 'There was an error upserting stops from this catalog entry. There were 0 upsertions from this entry.'
        else:
          print("________________________________OKAY!___________________________")
        # @TODO: record a log entry for bus stops that includes total number of lines in the stops.txt file plus the total number of rows updated or created from requestResults. These numbers should be equal but it will be good to see if they are not in order to investigate potential data issues.
        print("with catalogRow")
        updateChangeLog(getCatalogThumbPrint(catalogRow),BUS_UPSERT_ACTION,Message=requestResults,busNumbers=busLineDict)
          

def getMetadataFieldIfExists(fieldName, agencyFeedRow):
  if fieldName in agencyFeedRow:
    return agencyFeedRow[fieldName]
  return ""

def getMetadataUrlFieldIfExists(fieldName, agencyFeedRow):
  if fieldName not in agencyFeedRow:
    #updateInvalidUrlLog(agencyFeedRow, "N/A", fieldName + ": Field does not exist")
    updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), INVALID_URL_ACTION, Message=fieldName + ": Field does not exist",url="N/A")
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
  #    updateInvalidUrlLog(agencyFeedRow, agencyFeedRow[fieldName], "URL is invalid.")
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
  print("revision was called")
  print(fourfour)
  print(agencyFeedRow['agency_name'])
  fetchLinkZipFileUrl = getMetadataUrlFieldIfExists('fetch_link', agencyFeedRow)
  urlResponseIfValid = urlIsValid(fetchLinkZipFileUrl, agencyFeedRow)
  # Skip uploading to catalog if ZIP file is not valid
  if urlResponseIfValid == None: # This reports out on invalid GTFS urls
    return None
  
  
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
  #resp = requests.get(url=fetchLinkZipFileUrl)
  #bytes = resp.content
  # The above lines were used when calling a get request on the url twice for validation and use, but I changed it to call only once for both those things
  bytes = urlResponseIfValid.content
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
    #if catalogRow['name'] == "NTM: TEST: Pierce County Transportation Benefit Area Authority" or catalogRow['name'] == "TEST: Confederated Tribes of the Colville Indian Reservation" or 
    #if catalogRow['name'] == "NTM: TEST: City of Yakima, dba: Yakima Transit":
      #pdb.set_trace()
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
      else:
        return None


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
          if revision_response != None:
            #updateChangeLog(agencyFeedRow,CREATE_ACTION,None)
            updateChangeLog(getAgencyFeedThumbPrint(agencyFeedRow), CREATE_ACTION)
          
        else:
          print("replacing") 
          revision_response = revision(agencyFeedRowFourfour, agencyFeedRow)
          if revision_response != None:
            #updateChangeLog(agencyFeedRow,UPDATE_ACTION,agencyFeedRowFourfour)
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
  updateCatalog()
  updateTransitStopDataset()
  #resetTransitStopDataset() # Only uncomment this line when you want to clear out the stops entry in socrata
  
  with open('CHANGE_LOG.txt', 'w') as f:
    f.write(json.dumps(CHANGE_LOG, indent=4))

Main()

