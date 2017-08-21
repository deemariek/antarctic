#!/packages/python/current/bin/python
# -*- coding: UTF-8 -*-
# /usr/local/bin/python

"""
Created on Mon Jan 19 13:49:11 2015

@author: dky

Version 0.3

Get CCORE results data from their sftp server, copy new files to bslcene
Write all ccore results to one db table

20150615: Removed unnecessary functions to create views for separate sealion and bedrock views
20150615: Removed unnecessary function to create csv file of ccore results for sealion and bedrock results
20150727: Added check to see if a CSV file was on the server, tar.gz files are not downloaded
          Also added check to see if product is RS2 or TSX as that determined how filename is created
20150728: Added command line parameter to pass through a directory name that has already been downloaded
          extracted functionality to check csv contents to that could shared

"""

import pysftp 
import os
import psycopg2
import csv
import datetime
import time
import argparse
import sys
import commands
import re
# the monitoring results folder on bslcene
localFolder = "/data/polarview/ccore/monitoring_results/"

#----------------------------------------------------------

# Runtime error handler
def handleRuntimeError(s, tb):
  tbstr = ''
  for line in tb:
    tbstr += ' ===> ' + str(line[1]) + ' | ' + str(line[3]) + ' | Line: ' + str(line[2]) + ' | ' + str(line[4])
  msg = 'UNHANDLED RUNTIME ERROR: ' + str(s[1]) + ' (' + str(s[0]) + ') :: TRACEBACK :: ' + tbstr
  print msg

# ----------------------------------------------------------


def writeCCOREtoDBase(csvFile):

    fileName = csvFile.split('/')[3]
    print "Processing CCORE csv file in %s and writing data to database public.ccore_results_all" % (fileName)
    # connection to database and create table - ***real credentials removed before publishing to github
    conn = psycopg2.connect(database="database", user="user", password="password", host="host", port="port")
    curs = conn.cursor()
    with open(csvFile, 'rb') as csvfile:
        next(csvfile)
        bergreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        # skip a row if it has no coordinates - tested here against latitude
        for row in bergreader:
            # remove quotation marks which shouldn't be in the rows
            row = ' '.join(row).replace('"', '').split()
            if row[2] == ' ':
                pass
            else:
                try:
                    row_targetnum = None if row[1] == ' ' else row[1]
                    row_lat = None if row[2] == ' ' else row[2]
                    row_long = None if row[3] == ' ' else row[3]
                    row_wkt = "POINT(" + str(row_long) + " " + str(row_lat) + ")"
                    row_row = None if row[4] == '' else row[4]
                    row_column = None if row[5] == '' else row[5]
                    row_area = None if row[6] == '' else row[6]
                    row_majaxis = None if row[7] == '' else row[7]
                    row_minaxis = None if row[8] == '' else row[8]
                    row_wrlnLen = None if row[9] == '' else row[9]
                    row_detconf = row[10].capitalize()
                    row_detclass = row[11].capitalize()
                    row_origname = row[12]
                    source = "ccore"
                    if row_origname.startswith('RS2'):
                        origname = row[12].split('_')
                        date = origname[5]
                        time = origname[6]
                        format = origname[4]
                        detectDateTime = datetime.datetime.strptime(date + time, '%Y%m%d%H%M%S')     
                        fileImageName = "RS2_SS_{0}_{1}_{2}_HH_1.8bit.jp2".format(date, time, format)
                    elif row_origname.startswith('TSX1'):
                        origname = row_origname.split('_')[10]
                        date = origname.split('T')[0]
                        time = origname.split('T')[1]
                        detectDateTime = datetime.datetime.strptime(date + time, '%Y%m%d%H%M%S')     
                        fileImageName = "TSX1_SAR_SC_S_{0}T{1}_S_1.8bit.jp2".format(date,time)
                    else: 
                        print "Source file format not recognised"
                    # writing to database
                    query = "INSERT INTO public.ccore_results_all (targetnum, filename, sourcerow, sourcecol, area, maj_axis, min_axis, wrlnLen, detconf, detclass, acqtime, latlong, lat, long, prodtype, origfilename) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_Force_2d(ST_GeomFromText(%s, 4326)), %s, %s, %s, %s)"
#                     print query
                    curs.execute(query, (row_targetnum, fileImageName, row_row, row_column, row_area, row_majaxis, row_minaxis, row_wrlnLen, row_detconf, row_detclass, detectDateTime, row_wkt, row_lat[:6], row_long[:6], source, row_origname))
                except IndexError, e:
                    print "Attributes not assigned to the insert command. Error:", e
        # commit to the database
        conn.commit()           
        print "CCORE results written to database table ccore_results_all"
        curs.close()
        conn.close()

# -------------------------------------------------------

def checkCSVcontents(csvFile):
# check there are results in the csv file
# returns True is there is data, False is there is no data
    with open(csvFile, 'rb') as csvfile:
        next(csvfile)
        bergreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        # skip a row if it has no coordinates - tested here against latitude
        row_count = sum(1 for row in bergreader)
        if row_count == 0:
            return False
        elif row_count >= 1:
            return True

# ----------------------------------------------------------

def getCSVfromDir(directory):
# this function locates the csv file located in a passed directory 
# the function returns the csv filename if there are contents in the file
# the function returns false if the csv file has no contents
    try:
        newDirList = os.listdir(directory)
        # iterate through the new folder to find csv files and process them accordingly
        for f in newDirList:
            if f.endswith('.csv'):
                csvFilePath = os.path.join(directory, f)
                if checkCSVcontents(csvFilePath):
                    return csvFilePath
                else:
                    print "Error: CSV file has no data; not continuting"
                    for (dirpath, dirnames, filenames) in os.walk(directory):
                        for file in filenames:
                            os.remove(os.path.join(dirpath, file))
                    os.rmdir(directory)
                    return False
            else:
                pass
    except Exception, e:
      print "Error: data from {0} not written to db. Error: {1}" %(directory, e)
      return False
      

# ----------------------------------------------------------

def main():
    print "============================================================"
    print "START                                                       "
    print "============================================================"
    print time.strftime("%c")
    if args.dirName:
        dirPath = os.path.join(localFolder,args.dirName)
        if os.path.isdir(dirPath):
            print "Reading files from {0}".format(dirPath)
            csvFilePath = getCSVfromDir(dirPath)
            if csvFilePath:
                print "CSV file has data; writing data to database"
                writeCCOREtoDBase(csvFilePath)
            else:
                print "Downloaded files have been deleted."
        else:
            print "Error: the directory name provided, {0}, was not found in {1}".format(args.dirName, localFolder)
    else:
        # connect to sftp site
        with pysftp.Connection(host="ftpsys.ccore.mun.ca", username="BAS_Falkland_Admin", password="XtWU7bGn") as srv:
            # get the server directory and file listings
            serverData = srv.listdir()
            # get the local directory and file listings
            localData = os.listdir(localFolder)
            # isolate folders that are on the sftp site, but not the local server
            print "Comparing local files to server files"
            print
            s = set(localData)
            # missingDirs are the items on the ftp site that are not on the local server 
            missingDirs = [x for x in serverData if x not in s]
            # remove any csv files from the list of directories 
            for mF in missingDirs:
                if mF.endswith('.csv'):
                    missingDirs.remove(mF)
                else: 
                    pass
            if len(missingDirs) == 0:
                print "No new directories"
            else:
                print "%s new directories available" % (len(missingDirs))
                for m in missingDirs:
                    with srv.cd():
                        srv.chdir(m)
                        print "Examining contents of directory {0}".format(srv.getcwd())
                        dirFiles = srv.listdir() 
                        # check there is a csv file in the directory before continuing 
                        checkCSV = [x for x in dirFiles if x.endswith('.csv')]
                        if checkCSV:
                            print "CSV file present, continuing with download"
                            # make a new directory on the local server
                            newDir = os.path.join(localFolder, m)
                            os.mkdir(newDir)
                            print "Copying files from %s to local folder " % (m)
                            # Remove tar.gz files from download list
                            filesToCopy = [x for x in dirFiles if not x.endswith('tar.gz')]
                            # Copy the individual files that we want
                            for file in filesToCopy:
                                srv.get(file, localpath=os.path.join(newDir,file), preserve_mtime=True)
                            print "Downloaded {0} files".format(len(filesToCopy))
                            csvFilePath = getCSVfromDir(newDir)
                            if csvFilePath:
                                print "CSV file has data; writing data to database"
                                writeCCOREtoDBase(csvFilePath)
                            else: 
                                print "Downloaded files have been deleted."                            
                        else:
                            print "Error: no CSV file present on the server, not continuing"
    print "============================================================"
    print "END"
    print "============================================================"


# ----------------------------------------------------------


if __name__ == "__main__":

  parser = argparse.ArgumentParser(description=("Checks CCORE ftp server for new directories. "
                                                "Downloads new directories when the CSV file is made available. Writes data to a postgres table. "
                                                "Alternatively can write a directory to the database if the directory name is provided. "))
  parser.add_argument("-d", help="directory name of CCORE results to write to database.", action="store", dest='dirName')
  args = parser.parse_args()

  try:
    # Check if process already running - exit if it does
    output = commands.getoutput('ps -ef | grep "python"')
    i = 0
    for m in re.finditer('downloadCCORE_results', output ):
      i += 1
    if i > 1:
      print 'Process downloadCCORE_results.py already running -> exit'
    else:
      sys.exit(main())
  except Exception, e:
    # Handle runtime errors -> write traceback to log
    import inspect
    handleRuntimeError (sys.exc_info(), inspect.trace())
    raise
