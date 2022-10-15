import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings

# for recursive subfolder search
import os
from glob import glob
from itertools import chain

token = "Rk7bJ67t7NENazXHiV2iQCjkRuAz4jMKVFGN46oYHLJW6MHBkXYBePlUnS10o8d3iEBiy8TqOOGvg6qTMQhveg=="
bucket = "Benzin"
org = "fhswf"




result = (chain.from_iterable(glob(os.path.join(x[0], '*.txt')) for x in os.walk
                              ('C:\\Users\\SvenD\\Downloads')))




df = pd.read_csv('C:\\Users\\SvenD\\Downloads\\2022\\2022\\01\\2022-01-01-prices.csv', header=0,
                 index_col=("date"))




# =============================================================================
# 
# 
# with InfluxDBClient(url="http://localhost:8086/", token=token, org=org) as client:
#     """
#     Ingest DataFrame with default tags
#     """
#     point_settings = PointSettings(**{"type": "Benzin"})
#     point_settings.add_default_tag("example-name", "ingest-data-frame")
# 
#     write_api = client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)
#     write_api.write(bucket="benzin", record=df, data_frame_measurement_name="benzin_test_cases")
#     
# 
# =============================================================================
