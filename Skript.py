
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
token = "Usw_bLjmA4qJSYMM_vhpvQrBIL1lzfHHCNmtPVfTjQRs2VqatsZ-DRnHOPeRf3FcRXREq0XMRBttt32Yk8mwKQ=="
bucket = "benzin"
org = "fhswf"
root_path = Path(input('Enter the absolute path to the root into terminal drag the folder into the terminal:').replace("'",''))
def db_loader_station (station_list):
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=30000) as client:
        df = pd.read_csv(station_list, header=0)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket="benzin", record=df, data_frame_measurement_name="stations 19.10.2022",data_frame_tag_columns=['uuid'])
        client.close()


def db_loader (iterator_prices, granularity_string):
    t1 = time()
    #Establish DB connection
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=80000) as client:
        with client.write_api(write_options=WriteOptions(batch_size=500,
                                                        flush_interval=10_000,
                                                        jitter_interval=2_000,
                                                        retry_interval=5_000,
                                                        max_retries=5,
                                                        max_retry_delay=30_000,
                                                        exponential_base=2)) as write_client:
        
            #Iterate through the generator
            for x in iterator_prices:
                print(x)
                df = pd.read_csv(x, header=0,
                                     index_col=("date"))
                
                df.index = pd.to_datetime(df.index, utc=True)

                write_client.write(bucket="benzin", record=df, data_frame_measurement_name="gas_prices",data_frame_tag_columns=['station_uuid'])
            write_client.close()
        client.close()
    t2 = time()
    
    print(f"{granularity_string} took : {timedelta(seconds=(t2-t1))}")

def get_size(start_path = 'C:\persistent_space'):
    #Recursive, return file Size for folder in MB. 
    return sum(file.stat().st_size for file in Path(start_path).rglob('*'))*0.000001

def empty_bucket():
    start = "2000-01-01T00:00:00Z"
    stop = "2022-12-31T00:00:00Z"
    with InfluxDBClient(url="http://localhost:8086/", token=token, timeout=30000) as client:
        delete_api = client.delete_api()
        delete_api.delete(start, stop, '_measurement="gas_prices"', bucket=bucket, org=org)
        client.close()



#Crawling a dictionary and returning all .csv as a generator
path_month = os.path.join(root_path,"prices","2015","06")
month_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_month))

path_year = os.path.join(root_path,"prices","2015")
year_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_year))

path_full = os.path.join(root_path,"prices")
full_dataset_prices = chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(path_full))

latest_stations = os.path.join(root_path,"stations", "2022","10","2022-10-19-stations.csv")

#db_loader_station(latest_stations)
db_default_size = get_size()

#Getting starting db-size in from persistent space
db_loader(month_prices,"loading June 2015")
raw_month = get_size(path_month)
size_loaded_month = get_size()-db_default_size
print(f"raw size {raw_month:.3f}Mb | size in db:{size_loaded_month:.3f} Mb")
#Now we empty the bucket (delete the measurement "gas_prices")


#db_loader(year_prices,"Loading all of 2015")
#raw_year = get_size(year_prices)

#db_loader(year_prices,"Loading all of 2015")
#db_loader(full_dataset_prices,"The full dataset (up to 19.10.2022)")
#raw_full = get_size(full_dataset_prices)

