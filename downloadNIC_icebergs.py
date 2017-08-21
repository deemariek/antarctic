#!/packages/python/current/bin/python
# -*- coding: UTF-8 -*-
# /usr/local/bin/python

"""
Created on Mon Nov 24 11:42:11 2014

@author: dky
"""

# Version: 1.0 / 2015-01-05
#          1.1 / 2015-07-06 : changing intro comments
#          1.2 / 2015-07-10 : removing function to create icebergs' routes table
#          1.3 / 2015-07-13 : removed function that copied the downloaded file to the 'download' directory
# Author: dekel

# File to run to download the latest iceberg data from NIC
# Script downloads csv file if it is new, writes the data to a postgis DB table of all iceberg locations
# The script saves the csv file to the archive folder. 

# We have asked for lat long to be provided in decimal degress, however sometimes they still appear as DMS
# Use this code instead of lines 102 - 104
#                 lat = row[3]
#                 newlat = lat.replace('\xb0',"'").split("'")
#                 degreeslat = float(newlat[0])
#                 minuteslat = float(newlat[1])
#                 decimalsLat = degreeslat + minuteslat/60
#                 decimalsLat = round(decimalsLat, 4)
#                 if newlat[2] == 'S':
#                   decimalsLat = '-'+str(decimalsLat)
#                 
#                 long = row[4]
#                 newlong = long.replace('\xb0',"'").split("'")
#                 degreeslong = float(newlong[0])
#                 minuteslong = float(newlong[1])
#                 decimalsLng = degreeslong + minuteslong/60
#                 decimalsLong = round(decimalsLng, 4)
#                 if newlong[2] == 'W':
#                   decimalsLong = '-'+str(decimalsLong)
#                 wkt = "POINT("+str(decimalsLong)+" "+str(decimalsLat)+")"


import datetime
import urllib2
import os
import psycopg2
import csv
import shutil
import subprocess

#  address where the csv file is
downloadFile = "http://www.natice.noaa.gov/pub/icebergs/Iceberg_Tabular.csv"
# directories to write the files to
archiveDir="/data/polarview/custom/12_NICicebergs/archive/"

lastModifiedDate = 0

# ----------------------------------------------------------

def copyFileForDownload(source, dest): 
    # copy the archive file into the download folder
    if os.path.isfile(source) == True:
        if os.path.isfile(dest):
            os.remove(dest)
            print "Deleted previous iceberg file"
        else:
            pass
        shutil.copy2(source, dest)
        print "Copy of latest csv file made available for download"
    else:
        print "Copy not completed - source file not present - no file available for download" 

# ----------------------------------------------------------
# command to create the database table 
#   curs.execute("CREATE TABLE public.icebergs_s (id serial PRIMARY KEY, iceberg varchar(10), size varchar(10), remarks varchar(50), area int, lastupdated date, source varchar(10));")
#   curs.execute("SELECT AddGeometryColumn('public', 'icebergs_s', 'latlong', 4326, 'POINT', 2);")


def writeIcebergstoDBase(directory):

    print "Processing csv file in %s and writing data to database public.icebergs_s" %(directory)
    # connection to database and create table - ***real credentials removed before publishing to github
    conn = psycopg2.connect(database = "database", user="user", password="password", host="host.nerc-bas.ac.uk", port="port")
    curs = conn.cursor()
    updateLatest = "UPDATE icebergs_s SET latest = 'no' WHERE latest = 'yes';"
    curs.execute(updateLatest)

    # run through each line of the csv file and write relevant data to the database table
    with open(directory, 'rb') as csvfile:
     # skip the first line of the csv file which just has the column headings
        next(csvfile)
        bergreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        for row in bergreader:
            if row[0] == '':
                pass
            else:
                try:
                    print row
                    iceberg = row[0]
                    size = str(row[1]) + "X" + str(row[2])
                    area = int(row[1]) * int(row[2])
                    lat = row[3]
                    long = row[4]
                    wkt = "POINT("+str(long)+" "+str(lat)+")"
                    remarks = row[5]
                    # change the date format to suit postgres' date format requirements
                    updateDate = row[6]
                    # try/except statement to deal with if year format is '15' or '2015'
                    try:
                        lastupdated = datetime.datetime.strptime(updateDate, '%m/%d/%y').strftime('%Y/%m/%d')
                    except ValueError:
                        lastupdated = datetime.datetime.strptime(updateDate, '%m/%d/%Y').strftime('%Y/%m/%d')

                    # source hard coded for now - might have to update this is we start getting more iceberg data
                    source = "nic"

                    # show that this data is the most uptodate
                    latest = "yes"
                    # writing to database
                    query = "INSERT INTO public.icebergs_s (iceberg, size, remarks, area, lastupdated, source, latlong, latest) VALUES (%s, %s, %s, %s, %s, %s, ST_Force_2d(ST_GeomFromText(%s, 4326)), %s)" 
                    curs.execute(query,(iceberg, size, remarks, area, lastupdated, source, wkt, latest))
                except ValueError:
                    print "passed"
#     # commit to the database
    conn.commit()
    print "Iceberg data written to database" 
    # close database connection
    curs.close()
    conn.close()
    

# ----------------------------------------------------------

# this function can be used if the file has been successfully downloaded from the server, but not written to the database for whatever reason 
# eg if the csv file needs to be edited before it can be written to the database

# enter in new directory path
# fileName = "/data/polarview/custom/12_NICicebergs/archive/icebergs_2015_04_17.csv"

def getCSVfromFile(icebergFilePath):
    if os.path.isfile(icebergFilePath) == True:
        # update the icebergs database table
        writeIcebergstoDBase(icebergFilePath)
    else:
        print "Error with dir path %s" %(fileName)

# ----------------------------------------------------------

def main():
    # download the iceberg data file
    today = datetime.date.today()
    time = datetime.datetime.now().time()
    print "============================================================"
    print "START                                                       "
    print today, time
    print "============================================================"
    download_file(downloadFile)
    
#   getCSVfromFile(fileName)

    print "============================================================"
    print "END"
    print "============================================================"

# ----------------------------------------------------------

def download_file(download_url):
    response = urllib2.urlopen(download_url)
    headers = response.info()

    # take Last-Modified date from the csv's URL 
    csvDate = headers['Last-Modified']
    global lastModifiedDate
    lastModifiedDate = datetime.datetime.strptime(csvDate, '%a, %d %b %Y %X GMT').strftime('%Y_%m_%d')

    # create filename according to download date - save to archive
    icebergFile = "icebergs_"+lastModifiedDate+".csv"
    icebergFilePath = archiveDir+icebergFile
 
    # check filename it doesn't exist already
    if os.path.isfile(icebergFilePath) == False:
        print "Downloading most recent csv"
        file = open(icebergFilePath, 'w')
        file.write(response.read())
        file.close()
        subprocess.call(['chmod', '755', icebergFilePath])
        print "Download completed"
        # update the iceberg_s database table 
        writeIcebergstoDBase(icebergFilePath)

    else:
        print "Already downloaded"

# ----------------------------------------------------------


if __name__ == "__main__":
    main()
