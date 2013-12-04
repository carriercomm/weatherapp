## this will be the qc process for a inflow queue packet
## it is driven off the queue call back
## final result is returning the packet to a QC'ed in a pickled obs+qc dictionary

#!/usr/bin/env python
from netCDF4 import Dataset, stringtochar, chartostring
#from Scientific.IO.NetCDF import NetCDFFile as urlDataset
import cPickle as pickle
import json
import time ,calendar, datetime, os
import numpy as np
import pika
import sys
import fileinput
from threading import Thread, Lock
## most of this QC is from  http://madis.noaa.gov/madis_sfc_qc.html

# provide some limits and restrictions
# TODO : Move to a opendap netCDF file to generalize
validity={'temp':(233, 338), 'windv':(0, 128.3), 'windu':(0,360)}
temporal={'temp': 19.5, 'windv':25, 'windu':1000} # k/hr, mps/hr
ii=0

rabbitmq_server = 'vm-148-118.uc.futuregrid.org'

def L1_validity(obs):
    # this check is to compare against possible worldly values of measurement
    # as measurements are added to the stream these checks will need to be added also
    # -1 too large, -2 too small, 1 OK, 0 undefined
    if obs['key'] in validity.keys():
        (mmin, mmax)=validity[obs['key']]
        if float(obs['value'])>mmax:
            obs['qcL1Validity']=-1
        elif float(obs['value'])<mmin:
            obs['qcL1Validity']=-2
        else: 
            obs['qcL1Validity']=1
    else:
        obs['qcL1Validity']=0
    return obs
     
def L2_temporal(obs, stnhistory):
    #check within quick jump
    if obs['key'] in temporal.keys(): 
        if 'qcL2Temporal' not in obs.keys():              
                                    # if we care about the value
                for mkey in stnhistory.keys():     # for every time point
                    hist_value = float(stnhistory[mkey])
                    hist_time = int(mkey)
                    try:
                        mrate = (float(obs['value'])-hist_value)/((float(obs['timestamp'])-hist_time)*60*60) # rate per hour, type casted for safety
                    except:
                        print >> sys.stderr, "except"
                        mrate = 0 # assume a div by zero 
                        pass
                    if mrate < temporal[obs['key']]:
                        obs['qcL2Temporal']=1
                    else:
                        obs['qcL2Temporal']=-1
    
    return obs

def runqc(obs):
    # Open  
    #storagepath='http://ldm1.mcs.anl.gov:8080/opendap/aggregate/qualitycontrol/Last5ByStation/' # location
    storagepath='/ldm/var/data/aggregate/qualitycontrol/Last5ByStation/' # location
    fprefix = 'anl-qc-' # file class prefix
    filename = fprefix+"".join(x for x in obs['stn'] if x.isalnum())+".nc" # http://stackoverflow.com/a/295152 fast and nice
    t = time.time()
    #print str(ii)+" : "+storagepath+filename

    if (os.path.isfile(storagepath+filename)):
	print >> sys.stderr, "exists! and Ppen"
        rootcdf = Dataset(storagepath+filename, 'r')
        t = time.time()
        # Load the ship
        stnhistory = dict()
        ic=obs['key']
        if ic in rootcdf.variables.keys():  # if key exists in the pool
            for row in np.arange(0,len(rootcdf.dimensions['obs'])): # for each key 
                if np.isnan(rootcdf.variables[ic][row]): # if row is not a nan
                    stnhistory[rootcdf.variables['time'][row]]=rootcdf.variables[ic][row] # build history of values 
        rootcdf.close()
	# Search the ship
        obs['qcStarted']=time.time()
        obs = L1_validity(obs)
        obs = L2_temporal(obs, stnhistory)
        obs['qcLag']=(time.time()-obs['qcStarted'])*10**6
	
    # Head Home
    return obs


def callback(ch, method, properties, body):    
    global channel_final, lock
    obs = json.loads(body) # expand our object back
    #ch.basic_ack(delivery_tag = method.delivery_tag)
    print >> sys.stderr, " Received %r %r" % (obs['stn'], obs['key'],)
    obs = runqc(obs)
    #with lock:
        #channel_final.basic_publish(exchange='Outflow',routing_key='',body=pickle.dumps(obs), properties=pika.BasicProperties(delivery_mode=2,))
    #print pickle.dumps(obs)
    print json.dumps(obs)
    time.sleep(0.01) # sleep to yield cpu
    
def monitor():
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
    #channel = connection.channel()
    #queuename = 'ToBeQc'
    #channel.queue_declare(queue=queuename, durable=True)
    #channel.basic_consume(callback,
                          #queue=queuename)
    #channel.start_consuming()
    for line in fileinput.input():
        try:
            callback(None, None, None, line)
        except IndexError:
            pass
    
if __name__=="__main__":
    #connection_final = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
    #channel_final = connection_final.channel()
    lock=Lock()
#     for x in np.arange(0,1):
#         t = Thread(target=monitor)
#         t.start()
        
    monitor()
