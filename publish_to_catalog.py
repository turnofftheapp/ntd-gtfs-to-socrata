import pdb
import requests
import os
import json
from operator import itemgetter
from urllib.request import urlopen

# TODO: change this to DOMAIN_URL (so its recognized by Python as a constant)
domain_url = 'https://data.bts.gov'
AGENCY_FEED_DATASET_ID = "dw2s-2w2x"

credentials = (os.environ['SOCRATA_BTS_USERNAME'], os.environ['SOCRATA_BTS_PASSWORD']) 

def getMetadataFieldIfExists(fieldName, agencyFeedRow):
  if agencyFeedRow[fieldName]:
    return agencyFeedRow[fieldName]
  return ""

def getMetadataUrlFieldIfExists(fieldName, agencyFeedRow):
  if agencyFeedRow[fieldName]:
    if agencyFeedRow[fieldName]["url"]:
      return agencyFeedRow[fieldName]["url"]
  return ""

def setMetadata(set):
  description = "Agency Name: " + set['agency_name'] + "\n"
  description += "NTD ID: " + set['ntd_id'] + "\n"
  description += "Feed ID: " + set['feed_id'] + "\n"
  description += "GTFS: " + getMetadataFieldIfExists('has_gtfs', set) + "\n"
  description += "GTFS URL: " + getMetadataUrlFieldIfExists('fetch_link', set) + "\n"
  description += "Agency URL: " + getMetadataUrlFieldIfExists('agency_website', set) + "\n"
  description += "Region: " + getMetadataFieldIfExists('uza', set) + "\n"
  description += "City: " + getMetadataFieldIfExists('city', set) + "\n" # Update
  description += "State: " + getMetadataFieldIfExists('state', set) + "\n" # Update
  
  # @TODO: set all required metadata
  return { 
    'name': "NTM: " + set['ntd_name'],
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



#the parameter variable 'set' is one row in the dataset that represents a "source" of data from some city somewhere
def createNewRevision(set):
  ##########################
  ### Step 1: Create new revision
  ##########################
  metadata = setMetadata(set)
  permission = 'private'
  action_type = 'update' #Options are Update, Replace, or Delete 
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
  revision_source_type = 'upload' #'upload' for creating a new one and 'view' for using an existing set as the source
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
  source_response = requests.post(create_source_url, data=source_json, headers=headers, auth=credentials)

  ##########################
  ### Step 3: Upload File to source_type
  ##########################

  resp = requests.get(url=getMetadataUrlFieldIfExists('fetch_link', set))
  bytes = resp.content
  #pdb.set_trace()
  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{domain_url}{upload_uri}'
  upload_headers = { 'Content-Type': 'text/csv' }
  upload_response = requests.post(upload_url, data=bytes, headers=upload_headers, auth=credentials)


# 'fourfour' is the dataset ID of an existing dataset to update/replace
#the parameter variable 'set' is one row in the dataset that represents a "source" of data from some city somewhere
def updateRevision(fourfour, set):
  ########
  ### Step 1a: Create new revisionIn this step you will want to put the metadata you'd like to update in JSON format along with the action you'd like to take This sample shows the default public metadata fields, but you can also update custom and private metadata here.
  ########
  headers = { 'Content-Type': 'application/json' }
  revision_url = f'{domain_url}/api/publishing/v1/revision'
  action_type = 'replace'
  permission = 'private'
  metadata = setMetadata(set)
  body = json.dumps({
    'metadata': metadata,
      'action': {
        'type': action_type,
        'permission': permission
      }
  })

  update_revision_url = f'{revision_url}/{fourfour}'
  update_revision_response = requests.post(update_revision_url, data=body, headers=headers, auth=credentials)

  ####################
  create_source_uri = update_revision_response.json()['links']['create_source'] # It will also return the URL you need to create a source
  create_source_url = f'{domain_url}{create_source_uri}'


  ##########################
  ### Step 2: Create new source
  ##########################

  from datetime import datetime
  now = datetime.now().strftime("%Y-%m-%d")

  filename = set['ntd_id'] + " " + now + '.zip' 
  revision_source_type = 'upload'
  ##########################
  parse_source = 'true'
  ##########################
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
  resp = requests.get(url=getMetadataUrlFieldIfExists('fetch_link', set))
  bytes = resp.content
  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{domain_url}{upload_uri}'
  upload_headers = { 'Content-Type': 'text/csv' }
  upload_response = requests.post(upload_url, data=bytes, headers=upload_headers, auth=credentials)
  #pdb.set_trace()
  #########
  #Step 2a(5): Apply revisionHere you just apply your revision as you would if you were updating data.
  #########
  apply_revision_uri = update_revision_response.json()['links']['apply']
  apply_revision_url = f'{domain_url}{apply_revision_uri}'
  revision_number = update_revision_response.json()['resource']['revision_seq']

  body = json.dumps({
  'resource': {
      'id': revision_number
    }
  })
  #pdb.set_trace()
  apply_revision_response = requests.put(apply_revision_url, data=body, headers=headers, auth=credentials)
  pdb.set_trace()

"""
def replaceZip():
  dataset_name = 'cool_data'
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
  #Note: If your revision is a delete revision, a reivision with action_type "delete" to delete rows from the dataset, then the dataset is required to have a column designated as the primary id.
  #Step 2: Create new sourceIn this step you create a new source, to which you'll attach a file in step 3. Think of this as setting up the guidelines for where your data is going to come from and what it's going to look like.
  revision_source_type = 'upload' # Options are Upload (for uploading a new file) or View (for using the existing dataset as the source)
  parse_source = 'true' # Parsable file types are .csv, .tsv, .xls, .xlsx, .zip (shapefile), .json (GeoJSON), .geojson, .kml, .kmz. If uploading a blob file (ie: PDFs, pictures, etc.) parse_source will be false.
  filename = 'cool_dataset.csv'

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
  #Step 3: Upload File to source_typeIn this step, you actually pass the file to the source that you created in Step 2. In this example, a file is being passed from a local directory.
  with open('/Users/user.name/test_data/sample.csv', "rb") as f:
      bytes = f.read()
  f.closed

  upload_uri = source_response.json()['links']['bytes'] # Get the link for uploading bytes from your source response
  upload_url = f'{domain_url}{upload_uri}'
  upload_headers = { 'Content-Type': 'text/csv' }

  upload_response = requests.post(upload_url, data=bytes, headers=upload_headers, auth=credentials)
  #Step 4 (optional): Add new column to source before publishing (or modify existing column)This is an optional step. If you wanted to add a column or modify a column with a transform before uploading it to Socrata, you would modify the output_schema in this step. This sample will show the steps for adding a new column.For modifying a column, you would edit the transform on that column in your output colums. For full list of available transforms, see this documentation
  # Get the input schema that was created when you attached your file to your source in Step 3
  '''input_schemas = upload_response.json()['resource']['schemas']
  latest_input_schema = max(input_schemas, key=itemgetter('id'))

  # From there you can get the latest output schema (which will contain an array of columns you can modify)
  output_schemas = latest_input_schema['output_schemas']
  latest_output_schema = max(output_schemas, key=itemgetter('id'))

  # Modify the output columns that you just retrieved
  output_columns = latest_output_schema['output_columns']

  position = len(output_columns) + 1 # If you're adding a new column, position is a required field that determines the column order

  new_column = {
    'field_name': 'new_field_name',
    'display_name': 'New Display Name',
    'discription': '',
    'position': position,
    'transform': {
      'transform_expr': 'the text of your transform here'
    }
  }

  output_columns.append(new_column)

  output_columns_json = json.dumps({
  'output_columns':
    output_columns
  })

  # Get input schema url to post the data you've acquired and modified from your source response
  input_schema_id = latest_input_schema['id']
  input_schema_uri = source_response.json()['links']['input_schema_links']['transform'].format(input_schema_id=input_schema_id)
  input_schema_url = f'{domain_url}{input_schema_uri}'
  update_columns_response = requests.post(input_schema_url, data=output_columns_json, headers=headers, auth=credentials)
  '''
  #Step 5: Apply revision (publish)This is the final step in creating and publishing a new asset.
  apply_revision_uri = revision_response.json()['links']['apply']
  apply_revision_url = f'{domain_url}{apply_revision_uri}'
  print(f'APPLY URL {apply_revision_url}')

  revision_number = revision_response.json()['resource']['revision_seq'] # This number will always be 0 for the first publication. Then it will increment up by one each time a new revision is created.

  apply_revision_json = json.dumps({
  'resource': {
      'id': revision_number
    }
  })

  apply_revision_response = requests.put(apply_revision_url, data=apply_revision_json, headers=headers, auth=credentials)
  #Step 6 (optional): Update/EditNow if you'd like to start editing your existing datasets, you would begin again by first creating a revision and then a source. The difference this time is that you pass in your 4x4 dataset id. After you create the revision and the source, simply follow steps 3-5 to attach/modify your data and the publish.
  # First create the revision using the 4x4 of your dataset
  revision_json = json.dumps({
  'action': {
      'type': action_type
      }
  })

  update_revision_url = f'{revision_url}/{fourfour}'
  update_revision_response = requests.post(update_revision_url, data=revision_json, headers=headers, auth=credentials)

  # Then create the source using the revision that you just created.
  update_source_json = json.dumps({
    'source_type': {
      'type': revision_source_type,
      'filename': filename
    },
    'parse_options': {
      'parse_source': parse_source
    }
  })

  source_uri = update_revision_response.json()['links']['create_source']
  source_url = f'{domain_url}{source_uri}'
  update_source_response = requests.post(source_url, data=update_source_json, headers=headers, auth=credentials)


"""


































# This is the highest level function that takes in the data, iterates through it, 
# checking the field for the fourfour and deciding whether or not to create or update
# each row of data
def Main():
  agencyFeedResponse = requests.get("https://data.bts.gov/resource/" + AGENCY_FEED_DATASET_ID + ".json", headers={ 'Content-Type': 'application/json' }, auth=credentials)
  
  for agencyFeedRow in json.loads(agencyFeedResponse.content):
    
    #pdb.set_trace()
    
    # Only import feeds where original_consent_declined field is FALSE
    if 'original_consent_declined' in agencyFeedRow:
      if agencyFeedRow['original_consent_declined'] == False:

        # TEMP lines to comment/uncomment:
        print("creating")
        createNewRevision(agencyFeedRow)

        #pdb.set_trace()
        
        # print("updating")
        #updateRevision('9j55-uci8', agencyFeedRow) 

        # TODO: call function here  that looks through metadata to determine if datast exists and returns the given fourfour, if no fourfour returned, create a new dataset


Main()









