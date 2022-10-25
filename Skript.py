
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
token = "c5MX6xsL7dMMJl6jK_FMXa7Hg9z94SHMX7rre4KOTeLhK0RH0sezD8RnkftPBoS2yHBvvvImZZNylnjr-MhskQ=="
bucket = "benzin"
org = "fhswf"
root_path = Path(input('Enter the absolute path to the root into terminal drag the folder into the terminal:').replace("'",''))

def db_loader_station (station_list):
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=30000) as client:
        df = pd.read_csv(station_list, header=0)

        point_settings = PointSettings(**{"type": "Station"})

        write_api = client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)
        write_api.write(bucket="benzin", record=df, data_frame_measurement_name="stations 19.10.2022")


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

                point_settings = PointSettings(**{"type": "Price"})
                point_settings.add_default_tag("gas_prices", "ingest-data-frame")

                write_client.write(bucket="benzin", record=df, data_frame_measurement_name="benzin_test_cases")
    t2 = time()
    print(f"{granularity_string} took : {timedelta(seconds=(t2-t1))}")
    

#Crawling a dictionary and returning all .csv as a generator

#Raw Size = 207MB, Influx-DB size = 9,74MB (Measured by size of Docker container)
month_prices = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(os.path.join(root_path,"prices","2015","06"))))
#Raw Size = , Influx-DB size = 98,478MB (Measured by size of Docker container)
year_prices = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(os.path.join(root_path,"prices","2015"))))

full_dataset_prices = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(root_path,"prices")))

#Raw Size = 4.403MB, INFLUX-DB size = 36.078MB (Measured by size of Docker container)
latest_stations = os.path.join(root_path,"stations", "2022","10","2022-10-19-stations.csv")

#db_loader_station(latest_stations)

#db_loader(month_prices,"loading June 2015")
#db_loader(year_prices,"Loading all of 2015")
db_loader(full_dataset_prices,"The full dataset (up to 19.10.2022)")

