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
from zipfile import ZipFile

CREDENTIALS = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 
STANDARD_HEADERS = { 'Content-Type': 'application/json' }
UPLOAD_HEADERS = { 'Content-Type': 'text/csv' }
DOMAIN_URL = 'https://data.bts.gov'
AGENCY_FEED_DATASET_ID = "dw2s-2w2x"
CURRENT_CATALOG_LINK = "https://data.bts.gov/api/views/metadata/v1" # This is the link to all sets in the NTD catalog
CURRENT_CATALOG = json.loads(requests.get(CURRENT_CATALOG_LINK + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS).content)


# Just to read the source_responses more easily
def pPrint(object):
  print(json.dumps(object,indent=4))


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
  #headers = { 'Content-Type': 'application/json' }
  permission = 'private'
  metadata = setMetadata(agencyFeedRow)
  body = json.dumps({
    'metadata': metadata,
      'action': {
        'type': action_type,
        'permission': permission
      }
  })
  #update_revision_url = f'{revision_url}/{fourfour}'
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
  
  # The below is not working when updating.
  source_response = requests.post(create_source_url, data=source_json, headers=STANDARD_HEADERS, auth=CREDENTIALS)

  ##########################
  ### Step 3: Upload File to source_type
  ##########################
  resp = requests.get(url=getMetadataUrlFieldIfExists('fetch_link', agencyFeedRow))
  bytes = resp.content
  
  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{DOMAIN_URL}{upload_uri}'
  #upload_headers = { 'Content-Type': 'text/csv' }
  upload_response = requests.post(upload_url, data=bytes, headers=UPLOAD_HEADERS, auth=CREDENTIALS)
  pdb.set_trace()
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
  #apply_revision_response = requests.put(apply_revision_url, data=body, headers=STANDARD_HEADERS, auth=CREDENTIALS)
  #return apply_revision_response
  
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

# This scans the current catalog, and for each entry, looks for busStop data, and if any stops are not already in the 
# busStopEntry in the catalog, that busStop is added
def updateTransitStopDataset():
  #catalogBusStopData1 = requests.get(url=getMetadataUrlFieldIfExists('fetch_link', 'https://data.bts.gov/dataset/National-Transit-Map-All-Stop-Locations/39cr-5x89'))
  #catalogBusBytes1 = catalogBusStopData1.content

  allCatalogBusStops = requests.get(url='https://data.bts.gov/dataset/National-Transit-Map-All-Stop-Locations/39cr-5x89')
  catalogBusStopBytes = allCatalogBusStops.content

  # The below for loop iterates through the existing catalog, identifying entrys that we deal with in order to get their bus stop data
  # and add that data to the catalog bus stop data
  for catalogRow in CURRENT_CATALOG: 
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      print(catalogRow['description']) 
      catalogEntryZip = getZipUrl(catalogRow['description'])
      print(catalogEntryZip)
      
      # The below zipRequest contains multiple files. The stops.txt file must be gotten out of the content of this request
      # then, the stops.txt file can be iterated through and stops from it can be added to the 'allCatalogBusStops' by upserting them
      zipRequest = requests.get(url=catalogEntryZip)
      pdb.set_trace()



      catalogRow['id'] # This is a fourfour
      print(catalogRow['id'])
    



















'''
First attempt, but I realized that iterating through the catalog data made WAY more sense
  # agencyFeedResponse below is the incoming data that is being added to or changed in the NTDBTS catalog
  agencyFeedResponse = requests.get("https://data.bts.gov/resource/" + AGENCY_FEED_DATASET_ID + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS)
  
  for agencyFeedRow in json.loads(agencyFeedResponse.content):
    # Only import feeds where original_consent_declined field is FALSE
    #pdb.set_trace()
    if 'original_consent_declined' in agencyFeedRow:
      if agencyFeedRow['original_consent_declined'] == False:
        agencyFeedRowFourfour = getFourfourFromCatalogonMatchingFeedID(agencyFeedRow['feed_id'])
        metaDataLinkStart = 'https://data.bts.gov/api/views/metadata/v1/'
        metaDataUrl = f'{metaDataLinkStart}{agencyFeedRowFourfour}'
        metaDataRequest = requests.get(url=metaDataUrl, auth=CREDENTIALS)
        metaDataBytes = metaDataRequest.content
        
        decodedMetaData = metaDataBytes.decode('utf-8')
        print(type(decodedMetaData))
        zipUrl = getZipUrl(decodedMetaData)
        print('zipUrl')
        print(zipUrl)
     


        with ZipFile(zipFileBytes, 'r') as zipObject:
          listOfFileNames = zipObject.namelist()
          for fileName in listOfFileNames:
              if fileName == 'stops.txt':
                  # Extract a single file from zip
                  zipObject.extract(fileName, 'temp_py')
                  print('All the python files are extracted')

  '''



"""
  #Sudo code
    busStopData = query API for busStop data
    dataSets = query API for all datasets
    for agency in dataSets
      open ZIP file
      locate busStop file
      busStops = get busStop files data
      for each busStop in busStops
        if busStop is not in busStopData
          post busStop to busStopData
          importedAgencies.append(agencyWhereThisStopCameFrom)
        else
          pass
    return importedAgencies
  """


# This is the highest level function that takes in the data, iterates through it, 
# checking the field for the fourfour and deciding whether or not to create or update
# each row of data
def updateCatalog():
  # The below will be the change log that is emailed out once the script is finished running
  dataCreated = {}
  dataUpdated = {}
  changeLog = {"data created" : dataCreated, "data updated" : dataUpdated}
  



  # agencyFeedResponse below is the incoming data that is being added to or changed in the NTDBTS catalog
  agencyFeedResponse = requests.get("https://data.bts.gov/resource/" + AGENCY_FEED_DATASET_ID + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS)
  
  for agencyFeedRow in json.loads(agencyFeedResponse.content):
    # Only import feeds where original_consent_declined field is FALSE
    #pdb.set_trace()
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
          dataCreated[feedID] = changelogValue
          revision(None, agencyFeedRow)
        else:
          print("replacing")
          dataUpdated[feedID] = changelogValue
          revision(agencyFeedRowFourfour, agencyFeedRow)

        #pdb.set_trace()

        # Temporarily disabling 
        #revision(agencyFeedRowFourfour,agencyFeedRow)
        

def Main():
  #updateCatalog()
  updateTransitStopDataset()

Main()

