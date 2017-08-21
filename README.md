# antarctic
Python scripts completed whilst working at BAS

# downloadNIC_icebergs.py 
The script connects to the iceberg data released from http://www.natice.noaa.gov/, downloads the data and writes it to a postgis DB table.

There occasionally were issues with the download and processing of this data, so the getCSVfromFile() function can be invoked to run on a dataset already downloaded to the local server.

This data process is the underlying data behind the WFS feed of NIC iceberg data on http://www.polarview.aq/antarctic

# downloadCORE_results.py
This script checks data made available from the CORE ftp site, and downloads any newly available data. Occasionally an empty csv file  was made available on the ftp site so an additional check is carried out to ensure the directory cotntains valid files. The script writes the data contents to a postgis table, upon which sits a WFS service hosted in GeoServer. 

# imOGC.py
This script used in ICEMAR data processing which took new data products and ingested them into GeoServer and associate them with a layer group,  using curl commands invoked by the subprocess. The script can also delete particular products from GeoServer either individual products, or products from a declared time period.