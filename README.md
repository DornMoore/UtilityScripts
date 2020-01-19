# UtilityScripts
Utility scripts oterhs may find useful. If you find something here that helps you out, let me know - got improvements, please share.

### local_gsm2postgis.py
Basic script to import local GSM data (or any CSV) from a CSV file stored locally to a PostGIS enabled database. This will work for all Postgres databases, even without PostGIS. There are some notes if you need to adjust field names from the source table to the destination.

The script does not show an example of pushing the geom to the database, in my case, I have a trigger set on the database to update the geom whenever data is inserted or updated. But it would be nice to add it to the script. 
