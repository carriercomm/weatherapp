from lxml import etree
import sys
import time, random
import numpy as np
import urllib2, gzip
import dbutil2
from StringIO import StringIO
from threading import Thread

# station list
keys=np.array(['id', 'updated', 'lat', 'lon', 'tempf'])
stnlst = ['KILROMEO5','KILROMEO4','KILROMEO6','KILLOCKP9','KILLOCKP6','KILROMEO7','MLEMI2', 'KILCHICA130','KILCHICA86','KILCHICA132','KILCHICA30','KILCHICA106','KILCHICA114','KILCHICA52','KILCHICA112','KILCHICA60','KILCHICA105','KILCHICA69','KILCHICA110','KILCHICA68','KILOAKPA1','MAS937','KILCHICA131','KILSTICK3','KILCHICA115','KILCHICA37','KILCHICA51','KILCHICA116','KILCHICA125','KILHARWO2','KILLINCO6','KILEVANS2','KILLYONS1','KILRIVER4','KILEVANS3','KILCHICA54','KILLYONS3','MCHII2','KILBROOK2','MAT224','MAP878','MWYQ4356','KILLAKEB5','KILLAKEB4','KILGREEN4','KILLAKEB2','KILLAKEF3','KILLAKEF2','KILLIBER6','KILLIBER10','KILGURNE12','KILGURNE6','KILVERNO2','KILGURNE8','KILVERNO3','KILGURNE1','KILGURNE9','MC1569','KILHIGHL11','KILHIGHL6','KILZION3','KILZION5','KILZION4','KILZION6','KILTHIRD3','KILMUNDE8','KILLINDE2','KILDEERF2','KILGRAYS3','MUP139','KILLINDE3','KILMUNDE4','KILHIGHL10','KILDEERF3','KILLAKEV6','KILWINTH4','KILHIGHL13','KILWINNE3','KILWINNE6','MAU210','KILNORTH13','MAS935','KILNORTH9','KILCHICA109','MUP256','KILDESPL8','KILROSEM2','KILCHICA123','KILDESPL6','KILMCHEN11','KILJOHNS3','KILMCHEN6','KILMCHEN7','KILWONDE3','KILMCHEN9','KILCRYST6','KILRINGW2','KILINGLE2','KILRICHM3','KILGREEN9','KILCRYST7','MUP095','KILCARY4','KILSPRIN18','KILLAKEV4','KILRICHM2','KILWAUCO2','KILRICHM5','KILCARY3','KILLAKEZ6','MD8544','KILANTIO4','KILBARRI11','KILLAKEI3','KILBARRI4','KILWOODS10','KILLAKEI7','KILLAKEI8','KWITWINL3','MAR553','KILAUROR20','KILNAPER19','KILNAPER24','KILNAPER22','KILNAPER4','KILAUROR12','KILOSWEG9','KILNAPER9','KILNAPER21','KILWARRE2','KILNAPER15','KILOSWEG1','KILNAPER7','KILBATAV5','KILPLAIN19','KILBOLIN15','KILNAPER23','KILBATAV1','KILNORTH11','KILOSWEG6','KILBOLIN12','KILBOLIN13','KILBATAV3','KILBOLIN10','KILBOLIN11','KILWHEAT8','KILPLAIN6','KILBOLIN8','MUP431','KILLISLE3','MD1973','KILWINFI2','KILWHEAT5','KILWOODR4','KILWOODR1','KILSCHAU10','KILSCHAU1','KILHOFFM7','KILHOFFM1','MC0066','KILSCHAU9','KILSCHAU7','KILHANOV2','KILROSEL4','KILROLLI3','KILELKGR4','KILPALAT6','KILROSEL3','MD5203','KILHOFFM5','MAU553','KILARLIN4','KILPALAT10','KILARLIN6','KILARLIN3','KILPALAT4','KILBARTL1','KILCAROL6','KILMTPRO3','KILPALAT7','KILCAROL5','KILWESTC11','KILEASTD2','KILADDIS4','KILBARRI8','KILGLENE5','KILELGIN11','KILLAKEZ3','KILWESTC5','KILWESTC7','KILWESTC4','KILMELRO2','KILLAGRA2','KILELMHU5','KILGLENW1','KINMUNST1','KINMUNST4','KINHAMMO3','KINHAMMO2','KINGRIFF2','KILTINLE10','KINHAMMO5','MOBII2','KILOAKFO3','KINEASTC3','KINCEDAR4','KINWHITI4','KILFRANK1','KINCEDAR5','KILBEECH3','KINCROWN13','KILCHICA119','KILORLAN3','KINCEDAR3','KILORLAN2','KILORLAN5','KILORLAN6','ME0048','KILPALOS2','KILPEOTO1','KINCROWN12','KILORLAN7','MIN001','KILCHICA107','KILOAKLA4','KILPALOS4','MAT062','KILORLAN4','KILCLARE2','KILMOKEN1','INHAMMO5','MUP298','KINHOBAR5','KINPORTA5','KINPORTA3','KINOGDEN2']
lastupdate  = {stnlst[i]: 0 for i in range(0, len(stnlst))}

urlstart = 'http://stationdata.wunderground.com/cgi-bin/stationlookup?station='
urlend = '&format=xml&maxage=20&rand='#1373482597163&_=1373482597165' # time with 3  dec place removed

RUNNING = True

def monitor(stationindex, stationindexend, threadid):
    global RUNNING
    #  to loop and occasionaly fire off a request
    # assume at least 32+ stations in list intitally 200 so should be fine
    (connection, channel) = dbutil2.sdbconn()

    while RUNNING:
#        try:
            print >> sys.stderr, str(threadid)+": started :"+str(stationindex)+"-"+str(stationindexend)
            time.sleep(random.randint(20,25)) #sleep some random number 0-30 sec
            #print str(threadid)+": "+str(stnlst[stationindex:stationindexend])
            request(stnlst[stationindex:stationindexend], threadid, (connection,channel))
#        except Exception, e:
#            print "ERROR: Unhandled Exception Caught %s" % str(e)
#            pass # this covers us from random http/network errors
    dbutil2.sdbclose(connection)
    
def request(stnarray, threadid, conchan):
    # take care of pulling down xml files for stnarray
    # print "request"
    # myf = (open('out2.xml')).read()
    url = urlstart+",".join(stnarray)+urlend+"{0:.0f}".format((time.time()-6)*1000)+"&+=""{0:.0f}".format((time.time())*1000)
    #print url
    #print "+".join(stnarray)
    
    request = urllib2.Request(url) 
    request.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36') 
    request.add_header('Accept-encoding', 'gzip')
    opener = urllib2.build_opener()                                   
    response = opener.open(request) 
    if response.info().get('Content-Encoding') == 'gzip':
        buf = StringIO( response.read())
        f = gzip.GzipFile(fileobj=buf)
        myxml = f.read()
        #print str(threadid)+" : "+"{0:.0f}".format((time.time()))+": zipped"
    else:
        myxml = response.read()
        #print "not zipped"
    pxml(myxml, conchan)
    opener.close()
    
    
def pxml(inbound, conchan):
    # extract string into array 
    # this is real rough, but fast enough
    stump = etree.fromstring(inbound)
    connection = conchan[0]
    channel = conchan[1]
#    try: 
#    except:
#        time.sleep(.1) 
#        try:
#            (connection, channel) = dbutil2.sdbconn()
#        except:
#            pass
        
    for station in stump: # station level
#        print station.tag
        if time.time()-(lastupdate[station.tag])>(10*1): # 1 minute update interval
            stn =dict({})
            for measurement in station: # measurement level
#                print measurement.tag + " "+ measurement.attrib['val']
                stn[measurement.tag]=measurement.attrib['val']

            dbutil2.sdbinsert(channel, stn['id'], stn['lat'], stn['lon'], stn['elev'], 'temp', str((float(stn['tempf'])+459.67)*(5./9)), stn['updated'], 6)
            dbutil2.sdbinsert(channel, stn['id'], stn['lat'], stn['lon'], stn['elev'], 'windu', stn['winddir'], stn['updated'], 6)
            dbutil2.sdbinsert(channel, stn['id'], stn['lat'], stn['lon'], stn['elev'], 'windv', str(float(stn['windspeedmph'])*0.44704), stn['updated'], 6)            
            lastupdate[station.tag]=float(stn['updated'])
#        else:
#            print "station.tag : packet passed"
    del stump
    
   
if __name__=="__main__":

    #fire up some threads 
    for x in np.arange(0,8):
        stationindex=x*32
        stationindexend=(x+1)*32
        stationindexend=min(stationindexend, len(stnlst))
        t = Thread(target=monitor, args=(stationindex, stationindexend, x))
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        RUNNING = False

    

