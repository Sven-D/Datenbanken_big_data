import pandas as pd
#Establishing a connection to the servers, and writing into the DB
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings,  WriteOptions

from datetime import timedelta

# for recursive subfolder search
import os
from pathlib import Path
from os.path import abspath
from glob import glob
from itertools import chain


#Setting global parameters
token = "aytIw7piw_zuosqc0_6gtdiq5H97LdreOg3YbLefwIssvq0NoytU9uvpvSNPGwuw3irn4jEY-POWbGi-Kyk02Q=="
bucket = "fuel"
org = "fhswf"
#Enter path to root folder containing the tankerkoenig data 
root_path = Path('c:/Users/SvenD/Desktop/Datenbanken in Big Data/tankerkoenig-data')
#Enter path containing the volume of the docker container
persistent_space = Path('C:/persistent_space')


def read_query (start=0, stop=0):
    t1 = time()
    with InfluxDBClient(url="http://localhost:8086", token=token, org=org, timeout=5000000) as client:
        # Query: using Table structure
        tables = client.query_api().query(
            """  
            import "join"

            prices = 
            from(bucket: "fuel")
                |> range(start: 2015-01-22T00:00:00Z, stop:2015-04-23T00:00:00Z)
                |> filter(
                fn: (r) => r._measurement =="gas_prices" and r._field =="dieselchange" and r._value == 1 
                )
                |> keep(columns: ["_value","station_uuid"])
                |> count(column: "_value")
                |> group()

            stations = 
            from(bucket: "fuel")
                |> range(start: 1950-01-22T00:00:00Z, stop:1975-01-22T00:00:00Z)
                |> filter(
                fn: (r) => r._measurement =="stations"
                and r._field =="post_code" 
                and r._value == "59872"
                )
                |> unique()
                |> keep(columns: ["_field", "_value","uuid"])
                |> group()


            join.left(
                left: stations,
                right: prices,
                on: (l, r) => l.uuid == r.station_uuid,
                as: (l, r) => ({post_code: 
                l._value, station_uuid: l.uuid, Count_price_change: r._value})
            )

                    """
        )
        # Serialize to values
        output = tables.to_values(columns=['station_uuid', 'Count_price_change'])
        print(output)
    t2 = time()
    
    print(f"the query took : {timedelta(seconds=(t2-t1))}")

def db_loader_station (iterator_stations):
        with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=50000) as client:
            with client.write_api(write_options=SYNCHRONOUS) as write_api:
            
                #Iterate
                for x in iterator_stations:
                    
                    print(x)
                    df = pd.read_csv(x, header=0)
                        
                    df.index = pd.to_datetime(df.index, utc=True)
                    write_api.write(bucket=bucket, record=df, data_frame_measurement_name="stations",data_frame_tag_columns=['uuid'])
                    client.close()                   

def db_loader (iterator_prices, granularity_string):
    t1 = time()
    #Establish DB connection
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=50000) as client:
        with client.write_api(write_options=SYNCHRONOUS) as write_api:
        
            #Iterate
            for x in iterator_prices:
                
                print(x)
                df = pd.read_csv(x, header=0,
                                    index_col=("date"))
                    
                df.index = pd.to_datetime(df.index, utc=True)
                write_api.write(bucket=bucket, record=df, data_frame_measurement_name="gas_prices",data_frame_tag_columns=['station_uuid'])
                client.close() 
                

    t2 = time()
    
    print(f"{granularity_string} took : {timedelta(seconds=(t2-t1))}")

def get_size(start_path = persistent_space):
    #Recursive, return file Size for folder in MB. 
    return sum(file.stat().st_size for file in Path(start_path).rglob('*'))*0.000001

def empty_bucket():
    #We use this function to clear our bucket after each write process (by simply erasing and reinitializing it).
    start = "2000-01-01T00:00:00Z"
    stop = "2022-12-31T00:00:00Z"
    with InfluxDBClient(url="http://localhost:8086/", token=token, timeout=60000) as client:
        bucket_client = client.buckets_api()
        bucket_delete = bucket_client.find_bucket_by_name(bucket)
        bucket_client.delete_bucket(bucket_delete)
        bucket_client.create_bucket(bucket_name=bucket, org = org)
        
        
    client.close()



#Crawling a folder and returning a generator yielding the .csv data 
path_prices_month = os.path.join(root_path,"prices","2015","06")
month_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_prices_month))

path_prices_year = os.path.join(root_path,"prices","2015")
year_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_prices_year))

path_prices_full = os.path.join(root_path,"prices")
full_dataset_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_prices_full))

path_stations_full = os.path.join(root_path,"stations")
all_stations = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_stations_full))

#Getting starting db-size in from persistent space
db_default_size = get_size(persistent_space)

""" 
#Here we start the loading process of our differently sized data batches
db_loader(month_prices,"loading June 2015")
raw_month = get_size(path_prices_month)
size_loaded_month = get_size(persistent_space)-db_default_size
print(f"raw size {raw_month:.3f}Mb | size in db:{size_loaded_month:.3f} Mb")
#Now we empty the bucket
empty_bucket()

db_loader(year_prices,"Loading all of 2015")
raw_year = get_size(path_prices_year)
size_loaded_year = get_size(persistent_space)-db_default_size
print(f"raw size {raw_year:.3f}Mb | size in db:{size_loaded_year:.3f} Mb")
empty_bucket()

db_loader(full_dataset_prices,"The full dataset (up to 19.10.2022)")

"""

raw_full = get_size(path_prices_full)
size_loaded_full = get_size()-db_default_size
print(f"raw size {raw_full:.3f}Mb | size in db:{size_loaded_full:.3f} Mb")

print("Now we load all stations")
db_loader_station(all_stations)

read_query() 