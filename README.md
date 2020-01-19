# UtilityScripts
Utility scripts oterhs may find useful. If you find something here that helps you out, let me know - got improvements, please share.

### local_gsm2postgis.py
Basic script to import local GSM data (or really any csv) from a csv file stored locally to a postgis enabled database. This will work for all Postgres databases, even without postgis. There are some notes if you need to adjust field names from the source table to the destinamtion.
The script does not show an example of pushing the geom to the database, in my case, I have a triggere set on the datbase to update the geom whenever data is inserted or updated. But it would be nice to add it to the script. 
