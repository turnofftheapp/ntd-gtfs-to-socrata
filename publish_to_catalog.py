import pdb
import requests
import os
import json
from operator import itemgetter
from urllib.request import urlopen
from datetime import datetime
import re

CREDENTIALS = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 
STANDARD_HEADERS = { 'Content-Type': 'application/json' }
UPLOAD_HEADERS = { 'Content-Type': 'text/csv' }
DOMAIN_URL = 'https://data.bts.gov'
AGENCY_FEED_DATASET_ID = "dw2s-2w2x"
CURRENT_CATALOG_LINK = "https://data.bts.gov/api/views/metadata/v1" # This is the link to all sets in the NTD catalog
CURRENT_CATALOG = json.loads(requests.get(CURRENT_CATALOG_LINK + ".json", headers=STANDARD_HEADERS, auth=CREDENTIALS).content)


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
    'customFields': {
      'Common Core': {
        'Contact Email': 'NationalTransitMap@dot.gov',
        'Contact Name': 'Derald Dudley',
        'License': "here is a license",
        'Program Code': "code",
        'Publisher':"person",
        'Bureau Code': "other code",
        'Public Access Level': "10"
      }
    } 
  }

# 'fourfour' is the dataset ID of an existing dataset to update/replace
#the parameter variable 'set' is one row in the dataset that represents a "source" of data from some city somewhere
def revision(fourfour, agencyFeedRow):
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

  parse_source = 'false'

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
  #upload_headers = { 'Content-Type': 'text/csv' }
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
  


# Locates the FeedID within the description field of catalogRow and returns it. Returns None if not found
def getCatalogEntryFeedID(catalogRowDescription):
    regexLogic = re.compile('[\n]Feed ID: [0-9]+[\n]') # Defines the regex logic to be ran on the description of catalogRow to look for the FeedID
    regexResult = regexLogic.search(catalogRowDescription) # Applys the logic above to the actual description
    if regexResult == None:
      return None
    else:
      feedID = regexResult.group() # Querys the result for just what was found in the description based on the logic written in the re.compile() statement
      return feedID

# Takes in a row of incoming dataset metadata and iterates through the current catalog, looking for a matching feedID 
# in the catalog entries descriptions.
# Returns a fourfour if it finds a matching FeedID, returns null if no matching FeedID is found
def getFourfourFromCatalogonMatchingFeedID(incoming_feed_id):
  for catalogRow in CURRENT_CATALOG:
    if catalogRow['tags'] != None and 'national transit map' in catalogRow['tags']:
      if catalogRow['description'] == None:
        existingFeedID = None # Otherwise, we get an error when running getCatalogEntryFeedID on the row
      else:
        existingFeedID = getCatalogEntryFeedID(catalogRow['description']) # Identify FeedID in catalogRow
      
      if existingFeedID == incoming_feed_id: 
        return catalogRow['id'] # This is a fourfour
  return None
      
# This is the highest level function that takes in the data, iterates through it, 
# checking the field for the fourfour and deciding whether or not to create or update
# each row of data
def Main():
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
        else:
          print("replacing")
        revision(agencyFeedRowFourfour,agencyFeedRow)
        


Main()

