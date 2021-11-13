
import requests
import os
import json
from operator import itemgetter
from urllib.request import urlopen

domain_url = 'https://data.bts.gov'
credentials = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 

#parameter1 is 'upload' for creating a new one and 'view' for using an existing set as the source
#parameter2 is 'true' for parsing and 'false' for not. comes from this:
#parse_source = 'false' # Parsable file types are .csv, .tsv, .xls, .xlsx, .zip (shapefile), .json (GeoJSON), .geojson, .kml, .kmz. If uploading a blob file (ie: PDFs, pictures, etc.) parse_source will be false.
#in the request 
#source_response = requests.post(create_source_url, data=source_json, headers=headers, auth=credentials)
#parameter3 Options are Update, Replace, or Delete 
def createNewRevision(revision_source_type,parse_source,action_type,set):
  ##########################
  ### Step 1: Create new revision
  ##########################
  if set["gtfs"]:
    GTFS = "Yes"
  else:
    GTFS = "No"
  dataset_name = set['agency_name']
  description = "Agency name: " + set['agency_name'] + "\n"
  description += "Region: <region>\n"
  description += "City: " + set['city'] + "\n"
  description += "State: " + set['state'] + "\n"
  description += "NTD ID: " + set['ntd_id'] + "\n"
  description += "GTFS: " + GTFS + "\n"
  #MPO Name, City, or State (not present for all agencies)
  #NTD ID
  #GTFS (Y or N)
  #URL

  # @TODO: set all required metadata
  metadata = { 
    'name': dataset_name,
    'description': description,
    'customFields': {
      'Common Core':{
        'Contact Email': "test@email.com",
        'Contact Name': "GTFS User",
        'License': "here is a license",
        'Program Code': "code",
        'Publisher':"person",
        'Bureau Code': "other code",
        'Public Access Level': "10"
      }
    } 
  }

  action_type = 'update' # Options are Update, Replace, or Delete 
  # What is the difference between "Update" and "Replace"?
  permission = 'private'

  revision_json = json.dumps({
    'metadata': metadata,
    'action': {
      'type': action_type,
      'permission': permission
    }
  })

  headers = { 'Content-Type': 'application/json' }
  revision_url = f'{domain_url}/api/publishing/v1/revision'

  revision_response = requests.post(revision_url, data=revision_json, headers=headers, auth=credentials)

  fourfour = revision_response.json()['resource']['fourfour'] # Creating a new revision will return the 4x4 for your new dataset

  create_source_uri = revision_response.json()['links']['create_source'] # It will also return the URL you need to create a source
  create_source_url = f'{domain_url}{create_source_uri}'

  ##########################
  ### Step 2: Create new source
  ##########################
  # @TODO: replace NTDID with the NTD ID 
  from datetime import datetime
  now = datetime.now().strftime("%Y-%m-%d")

  filename = set['ntd_id'] + " " + now + '.zip' 
  source_json = json.dumps({
    'source_type': {
      'type': revision_source_type,
      'filename': filename
    },
    'parse_options': {
      'parse_source': parse_source
    }
  })
  source_response = requests.post(create_source_url, data=source_json, headers=headers, auth=credentials)

  ##########################
  ### Step 3: Upload File to source_type
  ##########################

  # @TODO
  resp = requests.get(url=set['gtfs_url']['url'])
  bytes = resp.content
  
  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{domain_url}{upload_uri}'
  upload_headers = { 'Content-Type': 'text/csv' }

  upload_response = requests.post(upload_url, data=bytes, headers=upload_headers, auth=credentials)

# This is the highest level function that takes in the data, iterates through it, 
# checking the field for the fourfour and deciding whether or not to create or update
# each row of data
def Main():
  testData = requests.get("https://data.bts.gov/resource/54k4-ny26.json", headers={ 'Content-Type': 'application/json' }, auth=credentials)
  output = json.loads(testData.content)
   
  for set in output:
    if 'fourfour' in set:
      createNewRevision('upload','false','update',set)
    else:
      createNewRevision('upload','false','update',set)




Main()






'''
def createNewRevisionOld():
  
  ##########################
  ### Step 1: Create new revision
  ##########################

  dataset_name = 'SetToBeUpdated'
  description = "Agency name: <agency>\n"
  description += "Region: <region>\n"
  description += "City: <city>\n"
  description += "State: <state>\n"
  #MPO Name, City, or State (not present for all agencies)
  #NTD ID
  #GTFS (Y or N)
  #URL

  # @TODO: set all required metadata
  metadata = { 
    'name': dataset_name,
    'description': description,
    'customFields': {
      'Common Core':{
        'Contact Email': "test@email.com",
        'Contact Name': "GTFS User",
        'License': "here is a license",
        'Program Code': "code",
        'Publisher':"person",
        'Bureau Code': "other code",
        'Public Access Level': "10"
      }
    } 
  }

  action_type = 'update' # Options are Update, Replace, or Delete 
  # What is the difference between "Update" and "Replace"?
  permission = 'private'

  revision_json = json.dumps({
    'metadata': metadata,
    'action': {
      'type': action_type,
      'permission': permission
    }
  })

  headers = { 'Content-Type': 'application/json' }
  revision_url = f'{domain_url}/api/publishing/v1/revision'

  revision_response = requests.post(revision_url, data=revision_json, headers=headers, auth=credentials)

  fourfour = revision_response.json()['resource']['fourfour'] # Creating a new revision will return the 4x4 for your new dataset

  create_source_uri = revision_response.json()['links']['create_source'] # It will also return the URL you need to create a source
  create_source_url = f'{domain_url}{create_source_uri}'


  ##########################
  ### Step 2: Create new source
  ##########################

  revision_source_type = 'upload' # Options are Upload (for uploading a new file) or View (for using the existing dataset as the source)
  parse_source = 'false' # Parsable file types are .csv, .tsv, .xls, .xlsx, .zip (shapefile), .json (GeoJSON), .geojson, .kml, .kmz. If uploading a blob file (ie: PDFs, pictures, etc.) parse_source will be false.

  # @TODO: replace NTDID with the NTD ID 
  from datetime import datetime
  now = datetime.now().strftime("%Y-%m-%d")

  filename = 'NTDID_' + now + '.zip' 
  source_json = json.dumps({
    'source_type': {
      'type': revision_source_type,
      'filename': filename
    },
    'parse_options': {
      'parse_source': parse_source
    }
  })
  source_response = requests.post(create_source_url, data=source_json, headers=headers, auth=credentials)


  ##########################
  ### Step 3: Upload File to source_type
  ##########################

  # @TODO
  resp = requests.get(url='https://yakimatransit.org/gtfs/yakima_gtfs.zip')
  bytes = resp.content
  
  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{domain_url}{upload_uri}'
  upload_headers = { 'Content-Type': 'text/csv' }

  upload_response = requests.post(upload_url, data=bytes, headers=upload_headers, auth=credentials)
'''





