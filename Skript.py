
import pandas as pd
#Establishing a connection to the servers, and writing into the DB
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings

from time import time
from datetime import timedelta

# for recursive subfolder search
import os
from pathlib import Path
from glob import glob
from itertools import chain


#Setting global parameters
token = "80lVAVV_krqomBP3xSx-9euqajzYtAnHc1GLBUH6L1TbuOfE0a3T-a1llh0IPvplhz01M-9xGDizEa7Gwr0uNA=="
bucket = "benzin"
org = "fhswf"
root_path = Path(input('Enter the absolute path to the root into terminal drag the folder into the terminal:').replace("'",''))

def db_loader (iterator, granularity_string):
    t1 = time()
    #Establish DB connection
    with InfluxDBClient(url="http://localhost:8086/", token=token, org=org, timeout=30000) as client:
            #Iterate through the generator
            print("Writing to Database")
            for x in iterator:

                tag = "Preis" if "Preis" in x else "Station"

                df = pd.read_csv(x, header=0,
                                     index_col=("date"))
                
                df.index = pd.to_datetime(df.index, utc=True)

                point_settings = PointSettings(**{"type": tag})
                point_settings.add_default_tag("Benzinpreise", "ingest-data-frame")

                write_api = client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)
                write_api.write(bucket="benzin", record=df, data_frame_measurement_name="benzin_test_cases")
    t2 = time()
    print(f"{granularity_string} took : {timedelta(seconds=(t2-t1))}")
    

#Crawling a dictionary and returning all .csv as a generator
month = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(os.path.join(root_path,"2015","06"))))
year = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(os.path.join(root_path,"2015"))))
full_dataset = (chain.from_iterable(glob(os.path.join(x[0], '*.csv')) for x in os.walk(root_path)))

db_loader(month,"loading June 2015")
db_loader(year,"Loading all of 2015")
db_loader(full_dataset,"The full dataset (up to 19.10.2022)")

