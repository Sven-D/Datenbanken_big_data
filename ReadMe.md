# What does the Script do? 

The script performs different interactions with an InfluxDB, based on the Dataset: 
[tankerkoenig-data - Repos (azure.com)](https://dev.azure.com/tankerkoenig/_git/tankerkoenig-data), with all the data up to 19.10.2022 being used.

Recommendation: Utilize a machine with at least 32GB of RAM, so there is plenty of memory available for the in-memory data.
___
### Additional Server Configuration: 
For a faster write to Disk, we set the snapshot duration to 10 seconds, as per recommendation for historical data: [Influx-Blog](https://www.influxdata.com/blog/tldr-influxdb-tech-tips-march-16-2017/)

--storage-cache-snapshot-write-cold-duration=10s

We also set the write timeout to 0, thus disabling it. 
--http-write-timeout=0

___
###  Write Query:

The different measurements of the write queries are solely calculated on the prices.

The following performance metrics are calculated per batch: 
 - Measure the Raw data size and compare it to the mounted volume of the docker container (With a subtracted default DB-size)
 - Measure time delta 

 -  batch sizes: 
	- Single Month (June 2015)
	 - Full Year (2015)
	 - Full Dataset 
___
Then it loads every station into the database as well.

### Read Query
A read query is calculated, which returns a JSON with a grouped Count of every price change referencing the different stations in Meschede (post code 59872). 
