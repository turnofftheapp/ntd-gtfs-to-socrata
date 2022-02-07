# National Transit Map: GTFS to Socrata Catalog

This script publishes GTFS-formatted data from transit agencies represented in the National Transit Database (NTD) to the https://data.bts.gov catalog. It creates one record in the data.bts.gov catalog for each agency and saves the current GTFS feed .zip file. It also updates the [National Transit Map - Map of Stop Locations](https://data.bts.gov/Public-Transit/National-Transit-Map-Map-of-All-Stop-Locations/dgzr-qn6a) dataset that visualizes transit stops on a nationwide map.

## Getting setup

1. Ensure you have the username and password for a Socrata account with [Publisher permissions](https://dev.socrata.com/publishers/).
1. Set the following environment variables:
	`SOCRATA_BTS_USERNAME`
	`SOCRATA_BTS_PASSWORD`

## Running the script

The script can be run in three different modes:
```
python3 publish_to_catalog.py catalog

python3 publish_to_catalog.py catalog_test

python3 publish_to_catalog.py stops_map
```

## Training videos

1. [Training Video: Catalog Script Overview](https://www.loom.com/share/fc0e353031444620b00f1a7567bf3c6b)
1. [Training Video: Updating The Agency Feed Dataset](https://www.loom.com/share/164b335d99ae4cb1bfd47b8ab327c6d4)

## Dataset links on data.bts.gov

- [NTM: National Transit Map - All Agency Feeds](https://data.bts.gov/Public-Transit/NTM-National-Transit-Map-All-Agency-Feeds/ymsd-c3s5): Public dataset with GTFS feeds for all transit agencies in the NTM.
- [National Transit Map - Map of All Stop Locations](https://data.bts.gov/Public-Transit/National-Transit-Map-Map-of-All-Stop-Locations/dgzr-qn6a): Dataset that visualizes NTM transit stops on a nationwide map.
- [NTM - PRIVATE: Ingest Script Log](https://data.bts.gov/Public-Transit/NTM-PRIVATE-Ingest-Script-Log/ngsm-beqg): Private dataset that records a record each time the ingest script has been run.

## Get in touch

Email [NationalTransitMap@dot.gov](mailto:NationalTransitMap@dot.gov) if you have questions, feedback, or you would like to contribute.

## Credits

This project developed as a partnership amongst the [U.S. Department of Transportation](https://www.transportation.gov/), [TOTAGO](https://www.totago.co), and [Trillium Solutions](https://trilliumtransit.com).

![USDOT Logo](https://user-images.githubusercontent.com/303765/152220770-efcbd997-636e-4ba8-80a7-4808ca331451.png)

![TOTAGO Logo](https://user-images.githubusercontent.com/303765/152220589-213e04e2-6271-4ddb-b9b5-7a4da7a65cbf.png)

![Trillium Logo](https://user-images.githubusercontent.com/303765/152220629-9f95e5f3-83c4-4bab-bc57-1fc14eb2faac.png)
