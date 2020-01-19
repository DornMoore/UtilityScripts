#!/usr/bin/env python

"""
local_gsm2postgis.py:
Script reads through locally stored gsm data from a csv file and imports
those data to our ICF database.

The script may also require that the computer being used has OGR properly
installed and the python connections to OGR as well. This link may help:
https://pythongisandstuff.wordpress.com/2016/04/13/installing-gdal-ogr-for-python-on-windows/"""

import csv
import json
import logging
import os

# import requests
import pandas as pd
# geoalchemy2 needed if your Postgresql table contains a geom field - even if you aren't pushing data to the field.
from geoalchemy2 import Geometry
from sqlalchemy import Column, ForeignKey, MetaData, Table, create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker

__author__ = "Dorn Moore, International Crane Foundation"
__credits__ = ["Dorn Moore"]
__license__ = "GPL"
__version__ = "1.1"
__maintainer__ = "Dorn Moore"
__email__ = "dorn@savingcranes.org"
__status__ = "Beta"

# ---- Define the Parameters ---- #

# Database connection - NOTE PASSWORD HERE SO MAKE SURE IT IS SECURE

# EXAMPLE db_string = "postgres://myUser:myPassword@myHost:/myDatabase"
db_string = "postgres://myUser:myPassword@myHost:/myDatabase"      # Database connection string

mySchema = "schema name"        # specific name of your schema
myTable = "table name"          # table name where data will load


# Set the path where the csv files are stored
csvFolder = 'c:/Code/TEST'

# Set the location of the logfile
logfile = 'local_gsm_to_db.log'


# ---- Define the Scripts Functions ---- #


def logger(logfile):
    '''Logging Function'''

    # remove any existing logfile
    try:
        os.remove(logfile)
    except OSError:
        pass

    # Setup the logging criteria
    logging.basicConfig(filename=logfile, level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    # start the script logging
    logging.info('-- Script Started --')


def getData():
    '''
    Retrieve csv files from local source and load to database.
    The base assumes that the field names in the CSV table all exist in the destination table.
    Requires pandas and sqlalchemy - imported above.
    '''

    ''' 
        **Gather the destination data table Object**
        Use sqlalchemy to build an object based on the data table in postgresql 
        where we'll be pushing these data
    '''

    db = create_engine(db_string)
    Session = sessionmaker(db)
    session = Session()

    # produce our own MetaData object - identify the schema (may be 'public')
    metadata = MetaData(schema=mySchema)

    # Reflect ONLY the table where we'll be pushing data.
    metadata.reflect(db, only=[myTable])
    # metadata.reflect(db) #This will reflect the entire database

    # we can then produce a set of mappings from this MetaData.
    Base = automap_base(metadata=metadata)

    # calling prepare() just sets up mapped classes and relationships.
    Base.prepare()
    # Set myTable to be our class for uploading data
    myTable = Base.classes.myTable

    '''
    ** Get and parse the CSV files **
    '''

    # Change directory to the CSV folder
    os.chdir(csvFolder)

    # Get the directory and file listing
    data = os.listdir()

    if len(data) >= 1:

        # IMPORT the data to our database

        for i in data:  # For each file on the SFTP server
            if i.endswith('csv'):   # Limit it to CSV files
                print(i)            # print the file name
                with open(i, 'r') as f:  # With each csv file
                    df = pd.read_csv(f)  # read the file into our dataframe
                    # convert all the file names to lower case
                    df.columns = map(str.lower, df.columns)

                    # Remove unamed columns from the dataframe.
                    # https://stackoverflow.com/questions/43983622/remove-unnamed-columns-in-pandas-dataframe/43983654
                    df.drop(df.columns[df.columns.str.contains(
                        'unnamed', case=False)], axis=1, inplace=True)

                    # Get the recordCount for report
                    recordCount = len(df.index)
                    print(str(i[:6]) + ' - ' + str(recordCount))
                    # print(df.to_dict(orient="records"))

                    '''
                    ** Here we need to match the field names in our dataframe to the fields in the database **
                    we'll use something like this to map our fields - note this is a manual map, 
                    so if you change fields later, it will need updating.

                    df.rename(columns={"csvColumnName1":"dbColumnName1", "csvColumnName2":"dbColumnName2"},.....)
                    so that all of the df columns match the ones in the database. 

                    If you need to drop a column from the df, you can use
                    df.drop(columns=['columnA', 'columnB', ...])

                    '''

                    # Insert the data to the database from the CSV file
                    session.bulk_insert_mappings(
                        ornitela, df.to_dict(orient="records"))
                    session.commit()    # Don't for get to commit!

                '''
                RENAME (Move) the file on the CSV folder after it is processed.
                '''

                # Check to see if the processed folder exists, if not, make it
                if not os.path.exists(csvFolder+'/processed'):
                    os.makedirs(csvFolder+'/processed')

                # Rename/Move the file
                os.rename(csvFolder+'/'+i, csvFolder+'/processed/'+i)

                # print out how many rows added
                note = str(recordCount) + \
                    ' new records uploaded from ' + i + '.\n'
                print(note)
                logging.info(note)

    else:
        note = "No data found to import"
        logging.info(note)
        print(note)

    # Closes the connection
    session.close()  # Believe you'll need this back on when actively pushing data to database


# ---- Get Started ----#
# Call the logger
logger(logfile)  # call the logger function

# Get and import data
db = create_engine(db_string)  # CREATE THE ENGINE
getData()


# wrap up the logging.
logging.info('-- Script Completed --')

# UNCOMMENT LINES BELOW ONCE YOU HAVE DB_STRING ENTERED

# Close the database connection
db.dispose()
