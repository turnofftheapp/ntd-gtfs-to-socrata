# Import some stuff
from socrata.authorization import Authorization
from socrata import Socrata
import os

# Make an auth object
auth = Authorization(
  "data.bts.gov",
  os.environ['SOCRATA_BTS_USERNAME'],
  os.environ['SOCRATA_BTS_PASSWORD']
)

# This is our socrata object, using the auth variable from above
socrata = Socrata(auth)

# This will make our initial revision, on a view that doesn't yet exist
revision = socrata.new({'name': 'New GTFS dataset'})

# revision is a Revision object, we can print it
print(revision)

### File upload

# Using that revision, we can create an upload
upload = revision.create_upload('google_transit.zip')

# And print it
print(upload)



