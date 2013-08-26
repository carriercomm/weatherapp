# a netcdf wrapper class
from netCDF4 import Dataset, stringtochar, chartostring,stringtoarr
import time ,calendar, datetime 
import numpy as np
import os


class mynetcdf:
    def __init__(self, path, filename, rtype='full'):
        self.data = []
        self.path = path
        self.rtype=rtype
        self.stntoid=dict()
        self.instodid=dict()
        self.obstoid=dict()
        self.filename = filename
        self.openfile()
        
    
    def openfile(self):
        # takes care of opening files
        print self.path+self.filename
        if (os.path.isfile(self.path+self.filename)):
            # file exists
            self.rootcdf = Dataset(self.path+self.filename, 'a') # open a dataset
            if self.rtype=='full': 
                self.stns = chartostring(self.rootcdf.variables['station_name'][:])
                self.stns = (np.ma.masked_array(self.stns, self.stns=='')).compressed()
                self.stntoid = dict(zip(self.stns,np.arange(0,len(self.stns))))
            self.instodid.clear()
            self.obstoid.clear()            
            
        else:
            self.rootcdf = Dataset(self.path+self.filename, 'w') # open a dataset
            self.newfile()
            self.stntoid.clear()
            self.instodid.clear()
            self.obstoid.clear()
        
    def close(self):
	# closes file 
	self.rootcdf.close()
                
    def newfile(self):
        if self.rtype=='full':
            self.rootcdf.createDimension('station', 700) # just just general sizing for compat
        self.rootcdf.createDimension('obs')
        #self.rootcdf.createDimension('instrument',100) # General Sizing
        self.rootcdf.createDimension('meta_strlen', 30) 
        self.rootcdf.createDimension('name_strlen', 20)
        self.rootcdf.createDimension('unit_strlen', 10)
        # variables
        if self.rtype=='full':
            lon = self.rootcdf.createVariable('lon', 'f', ('station', ), least_significant_digit=5)
            lon.standard_name = "longitude"
            lon.long_name = "station longitude"
            lon.units = "degrees_east"
            lat = self.rootcdf.createVariable('lat', 'f', ('station', ), least_significant_digit=5)
            lat.standard_name = "latitude"
            lat.long_name = "station latitude"
            lat.units = "degrees_north"
            alt = self.rootcdf.createVariable('alt', 'f', ('station', ), least_significant_digit=2)
            alt.standard_name = "height"
            alt.long_name = "vertical distance above sea level"
            alt.units = "m"
            alt.positive = "up"
            alt.axis = "Z"
            stn = self.rootcdf.createVariable('station_name', 'c', ('station','name_strlen' ))
            stn.long_name ="station name"
            stn.cf_role="timeseries_id"
            stnifo = self.rootcdf.createVariable('station_info', 'i', ('station', ))
            stnifo.long_name="DataSource"
        
        #stnifo.details="0.null,1.mesonet noaa,2.metar noaa,3.metar unidata,4.aprswx,5.cwop,6.wu rapidfire"
        
        #self.rootcdf.createVariable('instrument_name', 'c', ('instrument', 'name_strlen' ))
        #self.rootcdf.createVariable('units', 'c', ('instrument', 'unit_strlen'))
        # would like to put history, calibration, manufacturer, etc data in here. 
        
        stni=self.rootcdf.createVariable('stationIndex', 'i', ('obs', ))
        stni.long_name="which station this obs is for"
        stni.instance_dimension="station"
        
        #self.rootcdf.createVariable('instrumentIndex', 'i', ('obs',))
        tm =self.rootcdf.createVariable('time', 'd', ('obs', ))
        tm.standard_name="time"
        tm.long_name = "time of measurement";
        tm.units = "seconds since 1970-01-01 00:00:00"
        
        at = self.rootcdf.createVariable('addtime', 'd', ('obs', ), least_significant_digit=2)
        at.standard_name = "addtime"
        at.long_name = "time of database aggregation"
        at.units= "seconds since 1970-01-01 00:00:00"
        
        temp=self.rootcdf.createVariable('temp', 'f', ('obs', ), least_significant_digit=2, fill_value=-999.9)
        temp.standard_name = "air_temperature";
        temp.units = "K"
        temp.coordinates = "time lat lon alt"
        if self.rtype=='full':  
            self.rootcdf.title = "Aggregation of Data";
            self.rootcdf.summary = "IL Region"
            self.rootcdf.keywords = "point"
            # Critical to any sort of normality. 
            # This Convention Line HAS TO BE IN THERE
            self.rootcdf.Conventions = "CF-1.6";
            self.rootcdf.standard_name_vocabulary = "CF-1.6";
            self.rootcdf.description = "Aggregation of Data";
            self.rootcdf.time_coordinate = "time";
            self.rootcdf.cdm_datatype = "Station";
            self.rootcdf.stationDimension = "station";
            self.rootcdf.station_id = "station_name";
            self.rootcdf.station_description = "station_name";
            self.rootcdf.latitude_coordinate = "lat";
            self.rootcdf.longitude_coordinate = "lon";
            self.rootcdf.altitude_coordinate = "alt";
            self.rootcdf.geospatial_lat_max = "90.0";
            self.rootcdf.geospatial_lat_min = "-90.0";
            self.rootcdf.geospatial_lon_max = "360.0";
            self.rootcdf.geospatial_lon_min = "0.0";
            self.rootcdf.time_coverage_start = str(int(time.time()))+" seconds since 1970-01-01 00 UTC";
            self.rootcdf.time_coverage_end = str(int(time.time())+60*60)+" seconds since 1970-01-01 00 UTC";
            self.rootcdf.observationDimension = "obs";
            self.rootcdf.featureType = 'timeSeries'
        else:
            self.rootcdf.title = "Station Data"
            self.rootcdf.Conventions = "CF-1.6";
        self.rootcdf.sync()

    def addstn(self, obs):
        # add a station given a observation dictionary
        mystn = obs['stn']
        if mystn not in self.stntoid.keys():
            # create station
            station_id = len(self.stntoid)
            self.rootcdf.variables['station_name'][station_id]=stringtoarr(mystn,20)
            self.rootcdf.variables['lat'][station_id]=obs['lat']
            self.rootcdf.variables['lon'][station_id]=obs['long']
            self.rootcdf.variables['alt'][station_id]=obs['elev']
            self.rootcdf.variables['station_info'][station_id]=obs['dsource']
            self.stntoid[mystn]=station_id
        else:
            # station is defined find it
            station_id = self.stntoid[mystn] # and back to int
        self.rootcdf.sync()
        return station_id
    def addobs(self,obs,station_id):
        # add a observation given a station id
        # this needs a rewrite
        
        # create qc columns
        for column in obs.keys():
            if column[:2]=='qc':
                if obs['key']+"_"+column not in self.rootcdf.variables.keys():
                    newv=self.rootcdf.createVariable(obs['key']+"_"+column, 'i', ('obs' ), fill_value=-999)
                    newv.standard_name = column;
                    #newv.long_name = "-1 for fail 1 for pass"
        # find instrument 
        myins = obs['key']
    #    if myins not in instoid.keys():
        if myins not in self.rootcdf.variables.keys():
            # create instrument
            newv=self.rootcdf.createVariable(myins, 'f', ('obs' ), fill_value=-999.9)
            newv.standard_name = myins;
            newv.units = myins+" SI Unit"
            newv.coordinates = "time lat lon alt"
        # add meta for instrument
        if myins+"_meta" not in self.rootcdf.variables.keys():
            
            newv=self.rootcdf.createVariable(myins+"_meta", 'c', ('obs','meta_strlen' ), fill_value=-999.9)
            newv.standard_name = myins+"_meta";
            newv.long_name = "Metadata for instrument"

        # add observation
        uni_obs_str=obs['stn']+str(obs['timestamp'])+str(obs['dsource'])
        if uni_obs_str in self.obstoid.keys():
            obs_id=self.obstoid[uni_obs_str]
            self.rootcdf.variables[myins][obs_id] = obs['value']
        else:
            if self.rtype=="full":
                obs_id = len(self.rootcdf.dimensions['obs'])
            else:
                # this allows the station files 
                # if greater than 5 replace oldest
                obsrow = self.rootcdf.variables['time'][:]
                if len(obsrow)>5:
                    obs_id =np.where(obsrow==max(obs))
                else:
                    obs_id = len(self.rootcdf.dimensions['obs'])
            
  
            self.rootcdf.variables['stationIndex'][obs_id]=int(station_id)
            self.rootcdf.variables['time'][obs_id] = obs['timestamp']
            self.rootcdf.variables['addtime'][obs_id] = obs['addtime']
            self.rootcdf.variables[myins][obs_id] = obs['value']
            self.obstoid[uni_obs_str]=obs_id
        for column in obs.keys():
            if column[:2]=='qc':
                self.rootcdf.variables[obs['key']+"_"+column][obs_id]=int(obs[column])    
        # insert Observation 
        self.rootcdf.sync()
                    
            
