
import pandas as pd
#Establishing a connection to the servers, and writing into the DB
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings

#For decorator
from time import time

# for recursive subfolder search
import os
from glob import glob
from itertools import chain


def db_loader (iterator, granularity_string):
    t1 = time()
    #Establish DB connection
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org) as client:
            #Iterate through the generator 
            for x in iterator: 
                
                tag = "Preis" if "Preis" in x else "Station"
                
                df = pd.read_csv(x, header=0,
                                     index_col=("date"))
                point_settings = PointSettings(**{"type": tag})
                point_settings.add_default_tag("Benzinpreise", "ingest-data-frame")
                
                write_api = client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)
                write_api.write(bucket="benzin", record=df, data_frame_measurement_name="benzin_test_cases")
    t2 = time() 
    print(f' {granularity_string} : {(t2-t1):.4f}s')


    

       
            
#Setting global parameters
token = "80lVAVV_krqomBP3xSx-9euqajzYtAnHc1GLBUH6L1TbuOfE0a3T-a1llh0IPvplhz01M-9xGDizEa7Gwr0uNA=="
bucket = "benzin"
org = "fhswf"
file_directory = 'C:\\Users\\Sven\\Downloads\\Benzin'

#Crawling a dictionary and returning all .csv as a generator 
result = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(file_directory)))
             
db_loader(result,"For a test")



