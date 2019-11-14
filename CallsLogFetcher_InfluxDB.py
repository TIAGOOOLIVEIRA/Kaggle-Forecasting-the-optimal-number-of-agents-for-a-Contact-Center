class InfluxFetcher:
    def __init__(self, usr, pwd, path):
	#Influx python API
	#!python -m pip install influxdb
	!python -m pip install influxdb

	import numpy as np
	import pandas as pd
	from pandas import read_csv
	from pandas import DataFrame
	from datetime import datetime, date
	from pandas import concat
	from influxdb import InfluxDBClient

	self.PATHFILE = path

	#Let's assume that phonecall database has already been created 	
	#CREATE DATABASE phonecall
	#USE phonecall	
	self.client = InfluxDBClient("localhost", "8086", usr, pwd, "phonecall")

    def loadTransf(self):
	data_hour = pd.read_csv(self.PATHFILE, sep=',')
	data_hour['time'] = pd.to_datetime(data_hour['interval']).dt.time
	data_hour['interval'] = pd.to_datetime(data_hour['interval'])
	data_hour['interval'] = data_hour['interval'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d', errors='ignore').date())
	data_hour['time'] = data_hour['time'].apply(lambda x: x.hour)

	#transform time in continuous number & groupby
	hourly = data_hour.groupby(['interval','time']).mean()

	hourly_ds = hourly.reset_index()

	hourly_ds['time'] = hourly_ds['time'].apply(lambda x: pd.datetime.strptime(x, '%H:%M:%S').time())

	hourly_ds['date_time'] = hourly_ds[['interval','time']].apply(lambda x: pd.datetime.combine(*list(x)),axis=1)

	hourly_ds_final = hourly_ds.drop(['time'], axis=1)

	hourly_ds_final = hourly_ds_final.reset_index()

	hourly_ds_final = hourly_ds_final.drop(['interval'], axis=1)

	hourly_ds_final = hourly_ds_final.drop(['index'], axis=1)

	hourly_ds_final.index = hourly_ds['date_time']

	#Casting agent_headcount from flot to Int. Avoid error continuous in the prediction
	hourly_ds_final['agent_headcount'] = hourly_ds_final['agent_headcount'].astype('int64', copy=False)

	#Casting total_calls from flot to Int. Avoid error continuous in the prediction
	hourly_ds_final['total_calls'] = hourly_ds_final['total_calls'].astype('int64', copy=False)

	#Casting missing_calls from flot to Int. Avoid error continuous in the prediction
	hourly_ds_final['missing_calls'] = hourly_ds_final['missing_calls'].astype('int64', copy=False)

	return hourly_ds_final

    def fetchData(self):
	hourly_ds_final = loadTransf()
	#https://influxdb-python.readthedocs.io/en/latest/examples.html
	for index, row in hourly_ds_final.iterrows():
		json_body = [
            		{
                	"measurement": "calls_hour",
                	"time": str(row['date_time']),
                    	"fields": {
                        "agent_headcount": row['agent_headcount'],
                        "total_calls": row['total_calls'], 
                        "total_calls_duration": row['total_calls_duration'], 
                        "missing_calls": row['missing_calls'], 
                        "available_time": row['available_time'], 
                        "away_time": row['away_time'], 
                        "busy_time": row['busy_time'], 
                        "on_a_call_time": row['on_a_call_time'], 
                        "after_call_work_time": row['after_call_work_time'], 
                        "total_handle_time": row['total_handle_time'], 
                        "occupancy_rate": row['occupancy_rate'], 
                        "utilization_rate": row['utilization_rate'], 
                        "shrinkage_rate": row['shrinkage_rate']}
                	}
            	]
    		self.client.write_points(json_body)
	


pwd = "tiagoooliveira"
usr = "tiagoooliveira"
path = "/dataset/train.csv"

finlux = InfluxFetcher(usr, pwd, path)

finlux.fetchData()



