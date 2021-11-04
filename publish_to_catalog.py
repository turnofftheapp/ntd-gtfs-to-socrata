import requests
import os
import json
from operator import itemgetter

domain_url = 'https://data.bts.gov'
credentials = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 


### Step 1: Create new revision
##########################

dataset_name = 'GTFS Test 12'
metadata = { 'name': dataset_name } # Minimum required metadata
action_type = 'update' # Options are Update, Replace, or Delete
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



### Step 2: Create new source
##########################

revision_source_type = 'upload' # Options are Upload (for uploading a new file) or View (for using the existing dataset as the source)
parse_source = 'false' # Parsable file types are .csv, .tsv, .xls, .xlsx, .zip (shapefile), .json (GeoJSON), .geojson, .kml, .kmz. If uploading a blob file (ie: PDFs, pictures, etc.) parse_source will be false.
filename = 'testgtfs.zip'

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


### Step 3: Upload File to source_type
##########################

with open('test.zip', "rb") as f:
    bytes = f.read()
f.closed

upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
upload_url = f'{domain_url}{upload_uri}'
upload_headers = { 'Content-Type': 'text/csv' }

upload_response = requests.post(upload_url, data=bytes, headers=upload_headers, auth=credentials)


