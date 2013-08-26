#!/usr/bin/env python
# Purpose: Pull items out of the Final QC queue and store them in a netCDF file based on the time of the observation
# TODO: move globals into a passed tuple
 
from netCDF4 import Dataset, stringtochar, chartostring, stringtoarr
import numpy as np
import cPickle as pickle
import os, datetime, time
import pika
from mynetcdf import mynetcdf

qcfiles = dict()
stnfiles = dict()

rabbitmq_server = 'vm-148-118.uc.futuregrid.org'

def recordobs(obs):
    # record the provided observation dict
    ct = datetime.datetime.fromtimestamp(int(obs['timestamp'])+18000) # cst to utc
    storagepath='/ldm/var/data/aggregate/qualitycontrol/' # location
    fprefix = 'anl-qc-' # file class prefix
    filename = fprefix+ct.strftime('%Y')+'-'+ct.strftime('%m')+'-'+ct.strftime('%d')+'-'+ct.strftime('%H')+'00.nc'
    if filename not in qcfiles.keys():
        rootcdf = mynetcdf(storagepath, filename)
        stn_id = rootcdf.addstn(obs)
        rootcdf.addobs(obs, stn_id)
        qcfiles[filename]=rootcdf

    else:
        rootcdf = qcfiles[filename]
        stn_id = rootcdf.addstn(obs)
        rootcdf.addobs(obs, stn_id)
        
    filename = fprefix+"".join(x for x in obs['stn'] if x.isalnum())+".nc" # http://stackoverflow.com/a/295152 fast and nice
    if filename not in stnfiles.keys():
        stncdf = mynetcdf(storagepath+'Last5ByStation/', filename, rtype='stn')
        stn_id = rootcdf.addstn(obs)
        stncdf.addobs(obs,'0')
	stncdf.close()
        #stnfiles[filename]=stncdf
    else:  
        stncdf = stnfiles[filename]
        stncdf.addobs(obs,'0')
        #stnfiles[filename]=stncdf


def callback(ch, method, properties, body):
    # queue call back function on event
    t = time.time()
    obs = pickle.loads(body) # expand our object back
    ch.basic_ack(delivery_tag = method.delivery_tag)
    recordobs(obs)
    print "{0:10.10f}".format(time.time()-t)
    time.sleep(0.001)

if __name__=="__main__":
    # we can assume this is the general execution case
 
    connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=rabbitmq_server))
    channel = connection.channel()
    channel.queue_declare(queue='Final', durable=True)
    os.umask(003)
    os.setgid(1004)
    os.seteuid(1004)
    # now that our initials are set lets move forward to some packets
    channel.basic_consume(callback,
                      queue='Final')
    channel.start_consuming()
    print "goodbye"

