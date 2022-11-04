import pandas as pd
from pathlib import Path
from datetime import timedelta
from time import timezone

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings,  WriteOptions, Point

from time import time
from datetime import timedelta, tzinfo


def db_loader_station (iterator_stations, token, org, bucket):
        with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=50000) as client:
            with client.write_api(write_options=SYNCHRONOUS) as write_api:
            
                for x in iterator_stations:
                    
                    df = pd.read_csv(x, header=0)
                        
                    df.index = pd.to_datetime(df.index, utc=True)
                    write_api.write(bucket=bucket, record=df, data_frame_measurement_name="stations",data_frame_tag_columns=['uuid'])
            client.close()                   

def db_loader (iterator_prices, granularity_string, token, org, bucket):
    t1 = time()
    #Establish DB connection and Push synchronous writes into the DB
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=50000) as client:
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
        
#iterate through all CSV Files, load them into a Data frame and Feed them into the api
            for x in iterator_prices:
                
                df = pd.read_csv(x, header=0,
                                    index_col=("date"))
                    
                df.index = pd.to_datetime(df.index, utc=True)

#We use the Uuid as a Tag, creating a unique series Key for every row
                write_api.write(bucket=bucket, record=df, data_frame_measurement_name="gas_prices",data_frame_tag_columns=['station_uuid'])
    
    client.close() 
    t2 = time()
    print(f"{granularity_string} took : {timedelta(seconds=(t2-t1))}")

def get_size(start_path):
    #Recursive, return file Size for folder in MB. 
    return sum(file.stat().st_size for file in Path(start_path).rglob('*'))*0.000001

def empty_bucket(token, org, bucket):
    #We use this function to clear our bucket after each write process (by simply erasing and reinitializing it).
    start = "2000-01-01T00:00:00Z"
    stop = "2022-12-31T00:00:00Z"
    with InfluxDBClient(url="http://localhost:8086/", token=token, timeout=60000) as client:
        bucket_client = client.buckets_api()
        bucket_delete = bucket_client.find_bucket_by_name(bucket)
        bucket_client.delete_bucket(bucket_delete)
        bucket_client.create_bucket(bucket_name=bucket, org = org)
        
        
    client.close()
