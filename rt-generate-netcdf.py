#!/usr/bin/env python
# Purpose: Pull items out of the Final QC queue and store them in a netCDF file based on the time of the observation

from netCDF4 import Dataset, stringtochar, chartostring, stringtoarr
import numpy as np
import cPickle as pickle
import os, datetime, time
import pika
import sys
import json
import fileinput
from mynetcdf import mynetcdf

PATH=os.environ.get('STORAGE', '/ldm/var/data/aggregate/realtime/')
RABBIT_HOST='vm-148-118.uc.futuregrid.org'

rtfiles = dict() 

def recordobs(obs):
    # take a observation dictionary and record it in a netcdf file
    ct = datetime.datetime.utcnow()
    storagepath=PATH
    fprefix = 'anl-realtime-' # file class prefix
    filename = fprefix+ct.strftime('%Y')+'-'+ct.strftime('%m')+'-'+ct.strftime('%d')+'-'+ct.strftime('%H')+'00.nc'
    print >> sys.stderr, "Writing %s" % filename
    if filename not in rtfiles.keys():
        rootcdf = mynetcdf(storagepath, filename)
        stn_id = rootcdf.addstn(obs)
        rootcdf.addobs(obs, stn_id)
	rtfiles[filename]=rootcdf

    else:
        rootcdf = rtfiles[filename]
        stn_id = rootcdf.addstn(obs)
        rootcdf.addobs(obs, stn_id)
    
    
def callback(ch, method, properties, body):
    #print " [x] Received %r" % (body,)
    t = time.time()
    #obs = pickle.loads(body) # expand our object back
    obs = json.loads(body) # expand our object back
    #ch.basic_ack(delivery_tag = method.delivery_tag)
    recordobs(obs)
    print >> sys.stderr, "Record at: {0:10.10f}".format(time.time()-t)
    time.sleep(0.001)

if __name__=="__main__":
    # we can assume this is the general execution case
    #connection = pika.BlockingConnection(pika.ConnectionParameters(
            #host=RABBIT_HOST))
    #channel = connection.channel()
    #channel.queue_declare(queue='rtqueue', durable=True)
    #os.umask(003)
    #os.setgid(1004)
    #os.seteuid(1004)

    # now that our initials are set lets move forward to some packets
    #channel.basic_consume(callback,
                      #queue='rtqueue')
    #channel.start_consuming()
    for line in fileinput.input():
        try:
            callback(None, None, None, line)
        except IndexError:
            pass
    print >> sys.stderr, "goodbye"

