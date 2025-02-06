from pyspark import SparkContext, sql
from pyspark.sql import SQLContext, Row

# Set up SparkContext object
sc = SparkContext(appName = 'Lab2_Ex4')

# Upload data files
temps = sc.textFile('BDA/input/temperature-readings.csv')
precips = sc.textFile('BDA/input/precipitation-readings.csv')

# Split each line into a list of strings
temp_obs = temps.map(lambda line: line.split(";"))
precip_obs = precips.map(lambda line: line.split(";"))

# Use RDDs to build Row objects to build the necessary data frames
station_temps = temp_obs.map(lambda x: Row(station = x[0], temperature = float(x[3])))
daily_precips = precip_obs.map(lambda x: Row(station = x[0], date = x[1], precipitation = float(x[3])))

# Setup SQLContext object and build dataframes
sqlc = SQLContext(sc)
temp_df = sqlc.createDataFrame(station_temps)
precip_df = sqlc.createDataFrame(daily_precips)

# Return max temperature readings at each station
temp_df = temp_df.groupBy("station").max("temperature")
temp_df = temp_df.withColumnRenamed("max(temperature)", "temperature")

# Return max daily precipitation reading at each station
precip_df = precip_df.groupBy("station", "date").sum("precipitation")
precip_df = precip_df.groupBy("station").max('sum(precipitation)')
precip_df = precip_df.withColumnRenamed("max(sum(precipitation))", "precipitation")
precip_df = precip_df.select("station", "precipitation")

# Join the two datasets by station ID
station_df = temp_df.join(precip_df, ["station"], "inner")

# Filter according to max temperature and precipitation readings
station_df = station_df.filter("temperature >= 25").filter("temperature <= 30").filter("precipitation >= 100").filter("precipitation <= 200")

# Export the output in a csv file
station_df.write.options(header='True', delimiter=';').csv('BDA/output/BDA_Lab2_Ex4_Output')


### OUTPUT ###

# The output for this exercise was empty, as none of the stations met both of the filter criteria