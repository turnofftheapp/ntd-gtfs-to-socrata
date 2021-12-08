# Import some stuff
from socrata.authorization import Authorization
from socrata import Socrata
import os
import pdb
import requests
import json
import re
from operator import itemgetter
from urllib.request import urlopen
from datetime import datetime

# Boilerplate...
# Make an auth object
auth = Authorization(
  "pete-test.test-socrata.com",
  os.environ['SOCRATA_BTS_USERNAME'],
  os.environ['SOCRATA_BTS_PASSWORD']
)

# This is how we create our view initially
with open('cool_dataset.csv', 'rb') as file:
    (revision, output) = Socrata(auth).create(
        name = "cool dataset",
        description = "a description"
    # Had it reading the actual cool_dataset.csv but it gets the same error:
    # Unexpected status 403 {'params': {}, 'message': 'You are not allowed to access that resource', 'key': 'forbidden'}
    ).csv("in,place,of,file","cool_dataset.csv")

revision.apply(output_schema = output)

# This will build a configuration using the same settings (file parsing and
# data transformation rules) that we used to get our output. The action
# that we will take will be "update", though it could also be "replace"
config = output.build_config("cool-dataset-config", "update")

# Now we need to save our configuration name and view id somewhere so we
# can update the view using our config
configuration_name = "cool-dataset-config"
view_id = revision.view_id()

# Now later, if we want to use that config to update our view, we just need the view and the configuration_name
socrata = Socrata(auth)
# The below fourfour is for Yakima Transit
view = socrata.views.lookup('e7b3-nb2w') # View will be the view we are updating with the new data

with open('updated-cool-dataset.csv', 'rb') as my_file:
    (revision, job) = socrata.using_config(
        configuration_name,
        view
    # puting .blob(my_file) here earns an error talking about there is no .blob()
    ).csv(my_file) 
    print(job) # Our update job is now running
