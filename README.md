OVERVIEW
--------

The app has a few basic components:

rtrapid.py - gets weather data from weather underground, and puts it on an
Inflow exchange which fans out to queues rtqueue and ToBeQc
rt-generate-netcdf.py - Pull items out of the Final QC queue and store them in a netCDF file based on the time of the observation
qc-process-packet.py - read messages from ToBeQc queue and send to Final queue
qc-generate-netcdf.py - read messages from Final queue and write netcdf files

QUEUES
------

The following queues are needed:

Final - bound to Outflow exchange
ToBeQc - bound to Inflow exchange
rtqueue - bound to Inflow exchange

These must be manually set up on rabbitmq

DATA
----

Must be shared between all nodes. This is done currently by setting up an NFS server, and sharing the 
ldm/var/data/ directory


INSTALLATION
-------------

The scripts in this directory can be run with the included supd script.


Required External Dependencies
 
In order to get the basic Hyrax 1.8.0 server running, you will need:
 
-Libcurl,
-The HDF5 C library version 1.8.4-patch1 or higher (1.8.8 or higher recommended) from
        ftp://ftp.hdfgroup.org/HDF5/current/src. Be sure to build with './configure --enable-hl --enable-shared'.
 
- The netCDF-4 C library from ftp://ftp.unidata.ucar.edu/pub/netcdf. Version 4.1.1 or higher is required (4.2 or higher recommended).
        Be sure to build with './configure --enable-netcdf-4 --enable-shared --enable-dap'.
 
-Java 1.6
 
-Tomcat 6.x
        apt-get install tomcat6
 
(apt-get install for these)
        -libxml2, libcurl, libreadline
 
To run the Hyrax server, you will need to download and install the following (from source or binary):
http://www.opendap.org/download/hyrax/1.8#get_it (source Files)
 
   
-libdap
-BES
-General Purpose Handlers (aka dap-server)
-One or more data handlers depending on which data you wish to serve
        ( These all follow, ./configure; make; make install )
        - NetCDF Handler
        - Free Form Handler
        - HDF5 Handler
        - Fileout NetCDF Handler
        - NcML Module
        - CSV Handler
        - XML Data Response
        - Fits Handler
-OLFS (which is a Java binary and runs on any computer with Java 1.6)  
        http://www.opendap.org/pub/olfs/olfs-1.9.5-webapp.tgz
        Unpack using 'tar -xvf filename' and follow the instructions in the README file.
        default: Typically just put *.war in /var/lib/tomcat6/webapps and restart tomcat
 
Edit the config files
http://docs.opendap.org/index.php/Hyrax_-_BES_Configuration
        - start cmd: besctl start
http://docs.opendap.org/index.php/Hyrax_-_OLFS_Configuration
        - start cmd: service tomcat start/restart



