# ntd-to-socrata-bts

Uploads GTFS data from the NTD (National Transit Database) to the https://data.bts.gov catalog

# Getting setup

1. Ensure you have the username and password for a Socrata account with Publisher permissions.
1. Set the following environment variables:
	`SOCRATA_BTS_USERNAME`
	`SOCRATA_BTS_PASSWORD`

# Running the script

The script can be run in two different modes:
```
python3 publish_to_catalog.py catalog

python3 publish_to_catalog.py stops_map
```

