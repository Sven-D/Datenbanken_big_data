import pandas as pd

#Establishing a connection to the servers, and writing into the DB
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings,  WriteOptions

#Time Measurement
from datetime import timedelta

# for recursive subfolder search
import os
from pathlib import Path
from os.path import abspath
from glob import glob
from itertools import chain

#Imports 
from Read_Query import *
from Write_Query import *


#Setting global parameters
token = "aytIw7piw_zuosqc0_6gtdiq5H97LdreOg3YbLefwIssvq0NoytU9uvpvSNPGwuw3irn4jEY-POWbGi-Kyk02Q=="
bucket = "fuel"
org = "fhswf"
#Enter path to root folder containing the tankerkoenig data 
root_path = Path('c:/Users/SvenD/Desktop/Datenbanken in Big Data/tankerkoenig-data')
#Enter path containing the volume of the docker container
persistent_space = Path('C:/persistent_space')

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

raw_full = get_size(path_prices_full)
size_loaded_full = get_size()-db_default_size
print(f"raw size {raw_full:.3f}Mb | size in db:{size_loaded_full:.3f} Mb")

print("Now we load all stations")
db_loader_station(all_stations)

read_query() 