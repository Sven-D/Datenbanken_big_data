# for recursive subfolder search
import os
from os.path import abspath
from glob import glob
from itertools import chain
 
from Read_Query import *
from Write_Query import *


#Setting global parameters
token = "jjAPvDZGuohgJh5GZD0vZML8D_Ags-Q3iNSL9aOixjxHkbKafiBnc7CyrZclTU6vtVMjh_owy1OU0oZFnSwvkQ=="
org = "fhswf"
bucket = "fuel"
#Enter path to root folder containing the tankerkoenig data 
root_path = Path('c:/Users/Sven/Desktop/Datenbanken in Big Data/tankerkoenig-data')
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

#Here we start the loading process of our differently sized batches
db_loader(month_prices,"loading June 2015", token, org, bucket)
raw_month = get_size(path_prices_month)
size_loaded_month = get_size(persistent_space)-db_default_size
print(f"raw size {raw_month:.3f}Mb | size in db:{size_loaded_month:.3f} Mb")
#Now we empty the bucket
empty_bucket(token, org, bucket)

db_loader(year_prices,"Loading all of 2015", token, org, bucket)
raw_year = get_size(path_prices_year)
size_loaded_year = get_size(persistent_space)-db_default_size
print(f"raw size {raw_year:.3f}Mb | size in db:{size_loaded_year:.3f} Mb")
empty_bucket(token, org, bucket)

db_loader(full_dataset_prices,"The full dataset (up to 19.10.2022)", token, org, bucket)
raw_full = get_size(path_prices_full)
size_loaded_full = get_size(persistent_space)-db_default_size
print(f"raw size {raw_full:.3f}Mb | size in db:{size_loaded_full:.3f} Mb")

print("Now we load all stations")
db_loader_station(all_stations, token, org, bucket)


read_query(token, org, bucket, start="2014-01-22T00:00:00Z", stop="2015-04-23T00:00:00Z") 
