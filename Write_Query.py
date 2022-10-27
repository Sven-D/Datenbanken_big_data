
import pandas as pd
#Establishing a connection to the servers, and writing into the DB
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings,  WriteOptions


from time import time
from datetime import timedelta

# for recursive subfolder search
import os
from pathlib import Path
from glob import glob
from itertools import chain


#Setting global parameters
token = "Zoh_0wWLNT6SFAWOhFcUp5O5v8YGPKRdkHlTWkTzhFd0zGMeKDCQzaSfb8xuoBjs_Ew0GzEif7yNyuuY46xg4g=="
bucket = "benzin"
org = "fhswf"

root_path = Path(input('Drag & drop the root folder into the terminal:').replace("'",''))
persistent_space = Path(input('Drag & drop the folder, containing the volume of the docker container:').replace("'",''))


def db_loader_station (station_list):
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=30000) as client:
        df = pd.read_csv(station_list, header=0)
        with client.write_api(write_options=WriteOptions(SYNCHRONOUS,
                                                        retry_interval=10_000,
                                                        max_retries=10,
                                                        max_retry_delay=20_000,
                                                        exponential_base=2)) as write_api:
            write_api.write(bucket="benzin", record=df, data_frame_measurement_name="stations 19.10.2022",data_frame_tag_columns=['uuid'])
    client.close()


def db_loader (iterator_prices, granularity_string):
    t1 = time()
    #Establish DB connection
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org) as client:
        with client.write_api(write_options=WriteOptions(SYNCHRONOUS,
                                                        retry_interval=10_000,
                                                        max_retries=10,
                                                        max_retry_delay=20_000,
                                                        exponential_base=2)) as write_api:
        
            #Iterate
            for x in iterator_prices:
                print(x)
                df = pd.read_csv(x, header=0,
                                    index_col=("date"))
                    
                df.index = pd.to_datetime(df.index, utc=True)

                write_api.write(bucket="benzin", record=df, data_frame_measurement_name="gas_prices",data_frame_tag_columns=['station_uuid'])
    client.close()
    t2 = time()
    
    print(f"{granularity_string} took : {timedelta(seconds=(t2-t1))}")

def get_size(start_path = 'C:\persistent_space'):
    #Recursive, return file Size for folder in MB. 
    return sum(file.stat().st_size for file in Path(start_path).rglob('*'))*0.000001

def empty_bucket():
    start = "2000-01-01T00:00:00Z"
    stop = "2022-12-31T00:00:00Z"
    with InfluxDBClient(url="http://localhost:8086/", token=token, timeout=60000) as client:
        bucket_client = client.buckets_api()
        bucket_delete = bucket_client.find_bucket_by_name(bucket)
        bucket_client.delete_bucket(bucket_delete)
        bucket_client.create_bucket(bucket_name=bucket, org = org)
        
        
    client.close()



#Crawling a dictionary and returning all .csv as a generator
path_month = os.path.join(root_path,"prices","2015","06")
month_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_month))

path_year = os.path.join(root_path,"prices","2015")
year_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_year))

path_full = os.path.join(root_path,"prices")
full_dataset_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_full))

latest_stations = os.path.join(root_path,"stations", "2022","10","2022-10-19-stations.csv")

db_default_size = get_size(persistent_space)


#Getting starting db-size in from persistent space
db_loader(month_prices,"loading June 2015")
raw_month = get_size(path_month)
size_loaded_month = get_size(persistent_space)-db_default_size
print(f"raw size {raw_month:.3f}Mb | size in db:{size_loaded_month:.3f} Mb")
#Now we empty the bucket (delete the measurement "gas_prices")


db_loader(year_prices,"Loading all of 2015")
raw_year = get_size(path_year)
size_loaded_year = get_size(persistent_space)-db_default_size
print(f"raw size {raw_year:.3f}Mb | size in db:{size_loaded_year:.3f} Mb")


db_loader(full_dataset_prices,"The full dataset (up to 19.10.2022)")
raw_full = get_size(full_dataset_prices)
size_loaded_full = get_size()-db_default_size
print(f"raw size {raw_full:.3f}Mb | size in db:{size_loaded_full:.3f} Mb")


db_loader_station(latest_stations)
