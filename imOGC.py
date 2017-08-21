#!/usr/local/bin/python

# Version: 0.1, 2015-06-24
#          0.2, 2015-07-01: ANCZ removing hard coded GS URL -> moving into imConfig
#          0.3, 2015-07-14: DEKEL added functions to append product layers to specific layer groups
#          0.4, 2015-07-23: DEKEL changed productEPSG to be the value of the productfeed instead of the individual product
#          0.5, 2015-07-27: DEKEL changed productEPSG back to the value of the individual product
#		   0.6, 2015-07-30: DEKEL product styles (default and additional) are now being added according to db contents
#          0.7, 2015-08-03: DEKEL added wrapper function to addProductToLayerGroup
#
#
# Author: dekel@bas.ac.uk
#
# This script: 
# Ingests products to GeoServer, creating OGC feeds
# Deletes layers from GeoServer
# Appends layers to product-specific layer groups

import psycopg2
import datetime
import os
import subprocess
import urllib
import requests
import sys
import argparse
import commands
import re
import xml.etree.ElementTree as ET

import imConfig
import imLogging
import imDB

# randomly assigned logModule variable, just so something is there for now
logModule = 27
authtuple = (imConfig.gsUser, imConfig.gsPassword)

#----------------------------------------------------------

# Runtime error handler
def handleRuntimeError(s, tb):
  tbstr = ''
  for line in tb:
    tbstr += ' ===> ' + str(line[1]) + ' | ' + str(line[3]) + ' | Line: ' + str(line[2]) + ' | ' + str(line[4])
  msg = 'UNHANDLED RUNTIME ERROR: ' + str(s[1]) + ' (' + str(s[0]) + ') :: TRACEBACK :: ' + tbstr
  print msg
  imLogging.write(logModule, 3, msg)

#----------------------------------------------------------

def appendToLayerGroupXML(pid, productFeed):
    """
    Function carries out a GET curl on the current layer group. 
    The response is converted into XML. The XML is iterated through and the new layer and its associated style is appended in the appropriate place.
    Function returns a string containing the XML-ish string of the updated layer group. 
    **Maybe write the contents of the returnLayer to a file as it might get quite big in memory?
    """
    newLayer = ('<published type="layer"><name>pid{0}</name><atom:link xmlns:atom="http://www.w3.org/2005/Atom" rel="alternate" href="{1}/geoserver/rest/layers/pid{0}.xml" type="application/xml"/></published>').format(pid, imConfig.gsURL)
    newStyle = ('<style><name>style_{0}</name><atom:link xmlns:atom="http://www.w3.org/2005/Atom" rel="alternate" href="{1}/geoserver/rest/styles/style_{0}.xml" type="application/xml"/></style>').format(productFeed, imConfig.gsURL)
    try:
        url = imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/layergroups/productGroup{0}'.format(productFeed)
        headers = {'Accept': 'text/xml'}
        r = requests.get(url, headers=headers, auth=authtuple)
        if r.status_code == 404:
            raise Exception('Curl get request failed')
        elif r.status_code == 200:
            root = ET.fromstring(r.content)
            for child in root.iter():
                if child.tag ==  'publishables': 
                    child.append((ET.fromstring(newLayer)))
                if child.tag ==  'styles': 
                    child.append((ET.fromstring(newStyle)))
            returnLayer = ET.tostring(root)
            return returnLayer
        else:
            return False
    except Exception, e:
        imLogging.write(logModule, 2, "Error adding pid{0} from its layergroup. {1}".format(pid, e))
        return False

#----------------------------------------------------------

def removeFromLayerGroupXML(pid, productFeed):
    try:
        url = imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/layergroups/productGroup{0}'.format(productFeed)
        headers = {'Accept': 'text/xml'}
        r = requests.get(url, headers=headers, auth=authtuple)
        if r.status_code == 404:
            raise Exception('Curl get request failed')
        elif r.status_code == 200:
            root = ET.fromstring(r.content)
            index = -1
            for child in root.iter():
                if child.tag == 'publishables': 
                    for c in child:
                        for d in c:
                            if d.text == 'pid{0}'.format(pid):
                                # get index of this element which is used to remove the related style element
                                index = list(child).index(c)
                                child.remove(c)
                if child.tag == 'styles': 
                    c = child[index]
                    child.remove(c)
            returnLayer = ET.tostring(root)
            return returnLayer
        else:
            return False
    except Exception, e:
        imLogging.write(logModule, 2, "Error removing pid{0} from its layergroup. {1}".format(pid, e))
        return False

#----------------------------------------------------------

def returnStyleInsert(styleName):
    """
    Function returns the text formatting required to apply style to layers in GeoServer
    """
    newStyle = ('<style><name>{0}</name><atom:link xmlns:atom="http://www.w3.org/2005/Atom" rel="alternate" href="http://geos.polarview.aq/geoserver/rest/styles/{0}.xml" type="application/xml"/></style>').format(styleName)
    return newStyle

#----------------------------------------------------------

def appendAdditionalLayerStyles(pid):
    """ Function returns the additional styles
    Takes the product ID as an argument
    Returns a string of the XML formatting required to add additional
    styles to a product in GeoServer
    """
    layerStyles = selectStylesFromDB(pid)
    imLogging.write(logModule, 2, "returned styles: {0}".format(layerStyles))
    try:
        productStyle = layerStyles.get(False)
        if productStyle:
            headStyle = '<styles class="linked-hash-set">{0}</styles>'
            newStyles = ''
            for pS in productStyle:
                newStyle = returnStyleInsert(pS)
                newStyles += newStyle
            headStyles = headStyle.format(newStyles)
            updateStyles = updateGSLayerStyle(pid, headStyles)
            return updateStyles
        else:
            imLogging.write(logModule, 2, "No alternative styles for pid{0}".format(pid))
            return False
    except Exception, e:
        imLogging.write(logModule, 2, "Error: {}".format(e))
        return False

#----------------------------------------------------------

def updateGSLayerGroup(file, productFeed):
    """ 
    Function updates the contents of the layer group.
    The function needs the contents of the new layer group to be passed through to it 
    """
    try:
        sub1 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-type: text/xml', '-d', file, imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/layergroups/productGroup{0}'.format(productFeed)], shell=False, stdout=subprocess.PIPE)
        sub1.wait()
        if sub1.returncode == 0:
            return True
    except Exception, e:
        return False
        
#----------------------------------------------------------     
        
def updateGSLayerStyle(pid, newStyles):
    """ 
    Function updates the additional styles of the layer pid.
    The function needs the contents of the additional styles to be passed to it 
    """
    try:
        sub1 = subprocess.Popen(['curl', '-u', 'polarview:plrvwweb', '-XPUT', '-H', 'Content-Type: text/xml', '-d', '<layer>{0}</layer>'.format(newStyles), 'http://geos.polarview.aq/geoserver/rest/layers/pid{0}.xml'.format(pid)], shell=False, stdout=subprocess.PIPE)
        sub1.wait()
        if sub1.returncode == 0:
            return True
    except Exception, e:
        print e
        return False
        
#----------------------------------------------------------

def updateProductLayerGroup(pid, productFeed, append):
    """
    Function to update specific layergroup, either adding or removing a product layer
    Checks the product exists in GeoServer, check the layergroup exists
    """
    if append:
        newLayerGroup = appendToLayerGroupXML(pid, productFeed)
        imLogging.write(logModule, 2, "Appended layer pid{0} to layergroup XML.".format(pid))
        try: 
            updateLayerGroup = updateGSLayerGroup(newLayerGroup, productFeed)
            if updateLayerGroup:
                imLogging.write(logModule, 2, "productGroup{0} updated successfully".format(productFeed, pid))
                newLayerGroup = None
                return True
            else:
                imLogging.write(logModule, 3, "Error updating productGroup{0}.".format(productFeed))
                return False
        except Exception, e:
            imLogging.write(logModule, 3, "Error updating productGroup{0}: {1}".format(productFeed, e))
            return False
    else:
        newLayerGroup = removeFromLayerGroupXML(pid, productFeed)
        imLogging.write(logModule, 2, "Removed layer pid{0} from layergroup XML.".format(pid))
        try: 
            updateLayerGroup = updateGSLayerGroup(newLayerGroup, productFeed)
            if updateLayerGroup:
                imLogging.write(logModule, 2, "productGroup{0} updated successfully".format(productFeed, pid))
                newLayerGroup = None
                return True
            else:
                imLogging.write(logModule, 3, "Error updating productGroup{0}.".format(productFeed))
                return False
        except Exception, e:
            imLogging.write(logModule, 3, "Error updating productGroup{0}: {1}".format(productFeed, e))
            return False
            
#----------------------------------------------------------            

def getPRJwkt(epsg):
   sr = "http://spatialreference.org"
   f=urllib.urlopen(sr + "/ref/epsg/{0}/prettywkt/".format(epsg))
   return (f.read())

#----------------------------------------------------------

def layerExistsInDB(itemID):
    """ 
    SQL query to check if a row with itemID exists in the icemar database
    """    
    checkQuery = "SELECT exists (SELECT true from public.product where product.id = {0});".format(itemID)
    return imDB.select(checkQuery)[0][0]

#----------------------------------------------------------

def selectIDFromDB(itemID):
    """ 
    SQL query on the database. Returns required attributes for the row with itemID
    """
    despatchQuery = "SELECT product.id, product.fsrid_epsg, productfeed.fid_producttype, imzprovider.dir || '/extract/' || imzprovider.filename || '/' || product.filename as prodpath, productfeed.fcode_fileformat, left(product.filename, char_length(product.filename) - 4) as prodname, product.fid_productfeed, productfeed.fsrid_epsg as feed_fsrid_epsg \
              FROM public.product \
              INNER JOIN public.productfeed ON (product.fid_productfeed = productfeed.id) \
              INNER JOIN public.imzprovider ON (product.id = imzprovider.fid_product) \
              where product.id = {0};"
    return imDB.select(despatchQuery.format(itemID))

#----------------------------------------------------------

def selectStylesFromDB(itemID):
    """ An SQL query on the database to determine the style(s) to be applied to the itemID product layer in GeoServer 
    The function returns a dict with styles grouped according to default (true) and additional (false) styles
    """
    styleQuery = "SELECT productfeedsld.prime, sld.name \
                    FROM public.sld, public.productfeedsld\
                    WHERE productfeedsld.fid_sld = sld.id \
                    AND productfeedsld.fid_productfeed = ( \
                        SELECT product.fid_productfeed \
                        FROM public.product\
                        where product.id = {0});"
    rows = imDB.select(styleQuery.format(itemID))  
    styles = dict()
    for line in rows:
        if line[0] in styles:
            styles[line[0]].append(line[1])
        else:
            styles[line[0]] = [line[1]]
    return styles

#----------------------------------------------------------

def selectDeleteLayersFromDB(startDate, endDate):
    """
    This function carries out a SQL query on the icemar database to find product layers 
    that are between the passed integers.
    The function returns an array of all the rows returned by the SQL query
    """
    selectQuery = ("SELECT id, counter, fid_productfeed, name, publishtime, uploadtime, validtime, filename, filesize "
                       "FROM public.product " 
                       "where publishtime::date < current_date - integer '{0}' and publishtime::date > current_date - integer '{1}' "
                       "order by publishtime desc;")
    return imDB.select(selectQuery.format(startDate, endDate))

#----------------------------------------------------------

def layerExistsInGeoServer(productID):
    """ 
    Function checks if a layer with productID exists in GeoServer
    Returns True or False
    """
    url = imConfig.gsURL + '/geoserver/rest/layers/pid{0}.xml'.format(productID)
    headers = {'Accept': 'text/xml'}
    r = requests.get(url, headers=headers, auth=authtuple)
    if r.status_code == 200:
        return True
    else:
        return False

#----------------------------------------------------------

def layerGroupExistsInGeoServer(productFeed):
    """ 
    Function checks if a layerGroup with productFeed ID exists in GeoServer
    Returns True or False
    """
    url = imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/layergroups/productGroup{0}.xml'.format(productFeed)
    headers = {'Accept': 'text/xml'}
    r = requests.get(url, headers=headers, auth=authtuple)
    if r.status_code == 200:
        return True
    else:
        return False

#----------------------------------------------------------

def getLayerTypeInGeoServer(productID): 
    """
    Function to check layer typr in GeoServer
    Returns whether the layer is a coverage (raster) or datastore (vector)
    """
    url = imConfig.gsURL + '/geoserver/rest/layers/pid{0}.xml'.format(productID)
    headers = {'Accept': 'text/xml'}
    r = requests.get(url, headers=headers, auth=authtuple)
    root = ET.fromstring(r.content)
    return root.find('type').text
        
#----------------------------------------------------------
      
def ingestGeoTIFFIntoGeoserver(productID, newProduct, nativeCRS, declaredSRS, productName, productStyle):

    try:
        # creates store, creates layer, calculates BBOX, product filename is still the layer name, no SRS, not enabled
        sub1 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-type: text/plain', '-d', "file://{0}".format(newProduct), imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/coveragestores/pid{0}/external.geotiff?configure=none?recalculate=nativebbox,latlonbbox'.format(productID)], shell=False, stdout=subprocess.PIPE)
        sub1.wait()
        if sub1.returncode == 0:
            # changes layer name to pid[id], applies SRS, and enabled layer
            sub2 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-Type: application/xml',  '-d', '<coverage><name>pid{0}</name><nativeCRS>{1}</nativeCRS><srs>{2}</srs><projectionPolicy>FORCE_DECLARED</projectionPolicy><enabled>true</enabled></coverage>'.format(productID, nativeCRS, declaredSRS), imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/coveragestores/pid{0}/coverages/{1}.xml'.format(productID, productName)], shell=False, stdout=subprocess.PIPE)
            sub2.wait()
            # applies specific styling 16bit SLD to layer
            sub3 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-Type: text/xml', '-d', '<layer><defaultStyle><name>{0}</name></defaultStyle></layer>'.format(productStyle), imConfig.gsURL + '/geoserver/rest/layers/pid{0}.xml'.format(productID)], shell=False, stdout=subprocess.PIPE)
            sub3.wait()
            if sub1.returncode == sub2.returncode == sub3.returncode:
                returnValue = True
            else:
                returnValue = False
                imLogging.write(logModule, 3, "Error when changing layer name and applying SLD for {0}. Subprocess return code {1} and {2}".format(productID, sub2.returncode, sub3.returncode))
        else:
            imLogging.write(logModule, 3, "Error when creating GeoServer coverage for {0}. Subprocess return code: {1}".format(productID, sub1.returncode))
            returnValue = False
    except:
        returnValue = False
        imLogging.write(logModule, 3, "Ingesting GeoTIFF to GeoServer failed for {0}".format(productID))
        
    return returnValue
#         
#----------------------------------------------------------

def ingestSHPIntoGeoserver(productID, newProduct, nativeCRS, declaredSRS, productName, productStyle):

    try:
        # creates store, creates layer
        sub1 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-type: text/plain', '-d', "file://{0}".format(newProduct), imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/datastores/pid{0}/external.shp'.format(productID)], shell=False, stdout=subprocess.PIPE)
        sub1.wait()
        if sub1.returncode == 0:
            # change layer name to pid[id], set CRS and SRS
            sub2 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-Type: application/xml', '-d', '<featureType><name>pid{0}</name><nativeCRS>{1}</nativeCRS><srs>{2}</srs><projectionPolicy>FORCE_DECLARED</projectionPolicy><enabled>true</enabled></featureType>'.format(productID, nativeCRS, declaredSRS), imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/datastores/pid{0}/featuretypes/{1}.xml'.format(productID, productName)], shell=False, stdout=subprocess.PIPE)
            sub2.wait()
            # set layer style
            sub3 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XPUT', '-H', 'Content-Type: text/xml', '-d', '<layer><defaultStyle><name>{0}</name></defaultStyle></layer>'.format(productStyle), imConfig.gsURL + '/geoserver/rest/layers/pid{0}.xml'.format(productID)], shell=False, stdout=subprocess.PIPE)
            sub3.wait()
            if sub1.returncode == sub2.returncode == sub3.returncode:
                returnValue = True
            else:
                returnValue = False
                imLogging.write(logModule, 3, "Error when changing layer name and applying SLD for {0}. Subprocess return code {1} and {2}".format(productID, sub2.returncode, sub3.returncode))
        else:
            imLogging.write(logModule, 3, "Error when creating GeoServer store for {0}. Subprocess return code: {1}".format(productID, sub1.returncode))
            returnValue = False
    except:
        returnValue = False
        imLogging.write(logModule, 3, "Ingesting shapefile to GeoServer failed for {0}".format(productID))
    
    return returnValue
            
            
#----------------------------------------------------------

def deleteCoverageFromGeoserver(coverageName): 
    """ 
    A function to delete a coverage from GeoServer. The coverage name of the coverage to be deleted is passed as an argument.
    A curl command removes the coverage and store.
    """
    try: 
        # to delete the published layer:
        sub1 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XDELETE', imConfig.gsURL + '/geoserver/rest/layers/pid{0}.tiff'.format(coverageName)])
        sub1.wait()
        if sub1.returncode == 0:
            # To remove the coverage from which you published the layer, then call: 
            sub2 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XDELETE', imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/coveragestores/pid{0}/coverages/pid{0}.xml'.format(coverageName)])
            sub2.wait()
            sub3 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XDELETE', imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/coveragestores/pid{0}'.format(coverageName)])
            sub3.wait()
            if sub1.returncode == sub2.returncode == sub3.returncode:
                returnValue = True
                imLogging.write(logModule, 2, "GeoServer coverage deleted: pid{0}".format(coverageName))
            else:
                returnValue = False
                imLogging.write(logModule, 3, "Deleting process was not completed successfully: pid{0}".format(coverageName))
        else:
            returnValue = False
            imLogging.write(logModule, 3, "GeoServer layer not deleted. Return value: {0}".format(sub1.returncode))
    except:
        returnValue = False
    
    return returnValue


#----------------------------------------------------------

def deleteDatastoreFromGeoserver(featureName): 
    """
    A separate function to delete a layer from GeoServer. The feature name of the layer to be deleted are passed as an argument. 
    A curl command removes the layer and store
    """
    try:
        # to delete the published layer:
        sub1 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XDELETE', imConfig.gsURL + '/geoserver/rest/layers/pid{0}.xml'.format(featureName)])
        sub1.wait()
        if sub1.returncode == 0:
            # To remove the store from which you published the layer, then call: 
            sub2 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XDELETE', imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/datastores/pid{0}/featuretypes/pid{0}.html'.format(featureName)])
            sub2.wait()
            sub3 = subprocess.Popen(['curl', '-u', '{0}:{1}'.format(imConfig.gsUser, imConfig.gsPassword), '-XDELETE', imConfig.gsURL + '/geoserver/rest/workspaces/icemar_products/datastores/pid{0}'.format(featureName)])
            sub3.wait()
            if sub1.returncode == sub2.returncode == sub3.returncode:
                returnValue = True
                imLogging.write(logModule, 2, "GeoServer store deleted: pid{0}".format(featureName))
            else:
                returnValue = False
                imLogging.write(logModule, 3, "Deleting process was not completed successfully for pid{0}".format(featureName))
        else:
            returnValue = False
            imLogging.write(logModule, 3, "GeoServer layer not deleted. Return value: {0}".format(sub1.returncode))
    except:
        returnValue = False
        
    return returnValue



#----------------------------------------------------------

def ingestProductToGeoServer(productID, productPath, nativeCRS, declaredSRS, productName, productStyle, productType):
    """
    Function to ingest a layer into GeoServer. 
    """
    try: 
        # Product type Product type 1 = SIGRID-3 or 2 = Vector data product
        if productType == 1 or productType == 2:
            ingest = ingestSHPIntoGeoserver(productID, productPath, nativeCRS, declaredSRS, productName, productStyle)
            if ingest:
                return True
            else:
                return False
        # Product type 3 = Geoferenced raster / image product
        elif productType == 3:
            ingest = ingestGeoTIFFIntoGeoserver(productID, productPath, nativeCRS, declaredSRS, productName, productStyle)
            if ingest:
                return True
            else:
                return False
    except:
        return False             

#----------------------------------------------------------

def deleteOGCFeed(productID):
    """
    Function to delete a layer in GeoServer, based on the productID argument 
    """
    checkGS = layerExistsInGeoServer(productID)
    if checkGS:
        row = selectIDFromDB(productID)
        productFeed = row[0]['fid_productfeed'] 
        try:
            if layerGroupExistsInGeoServer(productFeed):
                updateProductLayerGroup(productID, productFeed, False)
                imLogging.write(logModule, 2, "Layer pid{0} removed from productFeed{1}.".format(productID, productFeed))
            else:
                imLogging.write(logModule, 3, "No layer group exists for productFeed {0}.".format(productFeed))
            checkGSLayer = getLayerTypeInGeoServer(productID)
            if checkGSLayer == 'RASTER':
                if deleteCoverageFromGeoserver(productID):
                    return True
                else:
                    imLogging.write(logModule, 3, "Error deleting coverage pid{0} from GeoServer".format(productID))
                    return False
            elif checkGSLayer == 'VECTOR':
                if deleteDatastoreFromGeoserver(productID):
                    return True
                else:
                    imLogging.write(logModule, 3, "Error deleting store pid{0} from GeoServer".format(productID))
                    return False
            else: 
                raise Exception('GeoServer layer not recognised as either raster or vector.')
        except Exception, e:
            imLogging.write(logModule, 3, "Error deleting pid{0} from GeoServer. {1}".format(productID, e))
            return False
    else: 
        imLogging.write(logModule, 3, "Error: Layer pid{0} does not exist in GeoServer".format(productID))
        return False

#----------------------------------------------------------

def ingestOGCFeed(pid):
    """
    Full process to ingest a layer into GeoServer. The database id of the layer to be ingested is passed as an argument.
    """
    checkGS = layerExistsInGeoServer(pid)
    if checkGS:
        imLogging.write(logModule, 2, "Layername {0} already exists in GeoServer. Process will not continue.".format(pid))
    else:
        checkDB = layerExistsInDB(pid)
        if checkDB:
            row = selectIDFromDB(pid)
            styles = selectStylesFromDB(pid)
            try:
                productID = row[0]['id']
                productEPSG = row[0]['fsrid_epsg']
                productType = row[0]['fid_producttype']
                productPath = row[0]['prodpath']
                productEnd = row[0]['fcode_fileformat']
                # remove full stop if at the end of filename - leads to problems with GeoServer otherwise
                if row[0]['prodname'].endswith('.'):
                    productName = row[0]['prodname'][:-1]
                else:
                    productName = row[0]['prodname']
                nativeCRS = getPRJwkt(productEPSG)
                declaredSRS = "EPSG:{0}".format(productEPSG)
                try:
                    productStyle = styles.get(True)[0]
                except:
                    imLogging.write(logModule, 2, "Database contained no style records for pid{0}; defaulting to fid_productfeed {1}.".format(productID, row[0]['fid_productfeed']))
                    productStyle = "style_"+str((row[0]['fid_productfeed'])) 
                imLogging.write(logModule, 2, "Processing product with id {0} and name {1}".format(productID, productName))
                if not productName[0].isdigit():
                    if os.path.exists(productPath):
                        # check productType parameter is in ingestProducts list in imConfig
                        if productType in set(imConfig.ingestProducts): 
                            imLogging.write(logModule, 2, "Starting process to ingest product {0}".format(productID))
                            if ingestProductToGeoServer(productID, productPath, nativeCRS, declaredSRS, productName, productStyle, productType):
                                imLogging.write(logModule, 2, "Product {0} ingestion completed".format(productID))
                                if appendAdditionalLayerStyles(productID):
                                    imLogging.write(logModule, 2, "Additional product styles ingested for pid{0}".format(productID))
                                else:
                                    pass
                            else:
                                imLogging.write(logModule, 3, "Product {0} ingestion was not completed".format(productID))
                        else: 
                            imLogging.write(logModule, 3, "Product type not recognised: {0}".format(productEnd))
                    else:
                        imLogging.write(logModule, 3, "Product file does not exist, {0}".format(productPath))  
                else:
                    imLogging.write(logModule, 3, "File {0} begins with a digit; not processing to GeoServer.".format(productName))  
            except:
                imLogging.write(logModule, 3, "Error: were not assigned correctly from db for product {0}".format(pid))        
        else:
            imLogging.write(logModule, 3, "Product id {0} does not exist in the database".format(pid))


#----------------------------------------------------------

def addProductToLayerGroup(productID):
    """
    Function to add a layer in to its respective productFeed layer group 
    """
    checkGS = layerExistsInGeoServer(productID)
    if checkGS:
        row = selectIDFromDB(productID)
        productFeed = row[0]['fid_productfeed'] 
        try:
            if layerGroupExistsInGeoServer(productFeed):
                updateProductLayerGroup(productID, productFeed, True)
                imLogging.write(logModule, 2, "Layer pid{0} added to productFeed{1}.".format(productID, productFeed))
                return True
            else:
                imLogging.write(logModule, 3, "No layer group exists for productFeed {0}.".format(productFeed))
                return False
        except Exception, e:
            imLogging.write(logModule, 3, "Error appending pid{0} to layergroup {1}. {2}".format(productID, productFeed, e))
            return False
    else: 
        imLogging.write(logModule, 3, "Error: Layer pid{0} does not exist in GeoServer".format(pid))
        return False

#----------------------------------------------------------


def main():
    if args.pid:
        ingestOGCFeed(args.pid)    
    elif args.deleteID:
        deleteOGCFeed(args.deleteID) 
    elif args.pidLG:
        addProductToLayerGroup(args.pidLG) 
    elif args.daysLookBack: 
        daysLookBack = int(args.daysLookBack)
        daysKeep = int(args.daysKeep)
        count = 0
        today = datetime.datetime.now()
        previousDate = datetime.timedelta(days=daysLookBack)
        secondDate = datetime.timedelta(days=daysLookBack+daysKeep)
        before1 = (today - previousDate).date()
        before2 = (today - secondDate).date()
        imLogging.write(logModule, 2, "Fetching products from icemar database, between {0} and {1}".format(before1, before2))
        layersToDelete = selectDeleteLayersFromDB(daysLookBack, daysLookBack+daysKeep)
        for row in layersToDelete:
            pid = row[0]
            if layerExistsInGeoServer(pid):
                checkGSLayer = getLayerTypeInGeoServer(pid)
                if checkGSLayer == 'RASTER':
                    deleteCoverageFromGeoserver(pid)
                    count = count + 1
                elif checkGSLayer == 'VECTOR':
                    deleteDatastoreFromGeoserver(pid)
                    count = count + 1
            else:
               pass        
        if count < 1:
            imLogging.write(logModule, 2, "No layers deleted from GeoServer")
        else:
            imLogging.write(logModule, 2, "Number of layers deleted from GeoServer: {0}".format(count))
    else:
        imLogging.write(logModule, 3, "Error: No arguments passed. Pass either -a, -d, -l, -r or -k")
            

#----------------------------------------------------------

# Application entry point
if __name__ == "__main__":

  parser = argparse.ArgumentParser(description=("Ingests ICEMAR products into GeoServer when -a [productID] is passed. "
                                                "Delete a particular ICEMAR products from GeoServer when -d [productID] is passed "
                                                "Deletes all outdated products from GeoServer when -r [integer] is passed"))
  parser.add_argument("-a", help="Icemar db pid of product to ingest into GeoServer.", action="store", dest='pid')
  parser.add_argument("-d", help="Icemar db pid of product to delete from GeoServer.", action="store", dest='deleteID')
  parser.add_argument("-l", help="Icemar db pid of product to add to its GeoServer layergroup.", action="store", dest='pidLG')
  parser.add_argument("-r", help="Days past to start removing GeoServer layers.", action="store", dest='daysLookBack')
  parser.add_argument("-k", help="Days past to stop removing GeoServer layers.", action="store", dest='daysKeep', default=5)
  args = parser.parse_args()

  try:
    # Check if process already running - exit if it does
    output = commands.getoutput('ps -ef | grep "python"')
    i = 0
    for m in re.finditer('imOGC', output ):
      i += 1
    if i > 1:
      print 'Process imOGC.py already running -> exit'
    else:
      sys.exit(main())
  except Exception, e:
    # Handle runtime errors -> write traceback to log
    import inspect
    handleRuntimeError (sys.exc_info(), inspect.trace())
    raise
