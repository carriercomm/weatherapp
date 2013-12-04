# this file is to provide a simple unified interface to 
# the queue datastore concept and to unify the filtering process. 
import time
import pika
# apt-get install rabbitmq / pip install pika
import cPickle as pickle
import json
import threading

#rabbitmq_server = 'vm-148-123.uc.futuregrid.org'
rabbitmq_server = 'vm-148-118.uc.futuregrid.org'

def sdbconn(queue='rtqueue'):
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
    #channel = connection.channel()
    #channel.queue_declare(queue=queue, durable=True)
    #return (connection, channel)
    return (None, None)

def sdbclose(connection):
    
    #connection.close()
    #connection.disconnect()
    pass

lock = threading.RLock()
    
def sdbinsert(channel, stn, lat, along, elev, key, value, timestamp, dsource):
    # insert a key into the queue for a given spaital stn, lat, long 
    ## looks rough but it's an easy speed up 
    if float(lat)<43:
        if float(lat)>41:
            if float(along)<-87:
                if float(along)>-89:
                    obs = dict({})
                    obs['stn']=stn
                    obs['key']=key
                    obs['value']=value
                    obs['timestamp']=timestamp
                    obs['addtime']=time.time()
                    obs['lat']=lat
                    obs['long']=along
                    obs['elev']=elev
                    obs['dsource']=dsource
                    #channel.basic_publish(exchange='Inflow',routing_key='',body=pickle.dumps(obs), properties=pika.BasicProperties(delivery_mode=2,))
                    with lock:
                        print json.dumps(obs)
                    # insert observation into rabbitmq for later processing. insert as pickled dictionary into named exchange Inflow 

def sdb_obj_insert(channel, obj, dsource):
    # insert a objectinto a queue defined by channel
    #channel.basic_publich(exchange='Inflow',routing_key='', body=pickle.dumps(obj))
    pass
