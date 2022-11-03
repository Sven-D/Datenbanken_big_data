from influxdb_client import InfluxDBClient
from datetime import timedelta
from time import time

def read_query (token=None, org=None, bucket=None, start=None, stop=None):
    t1 = time()
    with InfluxDBClient(url="http://localhost:8086", token=token, org=org, timeout=5000000) as client:
        # Query: using Table structure
        tables = client.query_api().query(
            f"""  
            import "join"

            prices = 
            from(bucket: "fuel")
                |> range(start:{start}, stop:{stop})
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
                as: (l, r) => ({{post_code: 
                l._value, station_uuid: l.uuid, Count_price_change: r._value}})
            )

             """
        )
        # Serialize to values
        output = tables.to_json(indent=5)
        print(output)
    t2 = time()
    
    print(f"the query took : {timedelta(seconds=(t2-t1))}")