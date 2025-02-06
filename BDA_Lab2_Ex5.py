from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 3")
sqlContext = SQLContext(sc)

#reading precipitation data
rdd= sc.textFile("BDA/input/precipitation-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
precReadingsRow= parts.map(lambda p:(p[0],p[1],int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]),float(p[3])))

#reading station stations-Ostergotland output
rdd= sc.textFile("BDA/input/stations-Ostergotland.csv")
parts = rdd.map(lambda l: l.split(";"))
stationsRow = parts.map(lambda p: (p[0],p[1]))

# Define the schema
stationsString = ["stationNum", "stationName"]
precReadingsString = ["station", "date", "year", "month", "day", "value"]

# Apply the schema to the RDD and join data # ref: course slide
schemaStations = sqlContext.createDataFrame(stationsRow,stationsString)
schemaPrecReadings = sqlContext.createDataFrame(precReadingsRow,precReadingsString)
precStationOst = schemaStations.join(schemaPrecReadings,schemaStations['stationNum']==schemaPrecReadings['station'], 'inner')

#filter over year 1993-2016
precStationOst_filtered = precStationOst.filter((precStationOst["year"] >= 1960) & (precStationOst["year"] <= 2014))

#calculate the total monthly precipitation for each station
precipitation_sum_station_month = precStationOst_filtered.groupBy("year", "month","station").agg(F.sum("value").alias("precipitation_sum_over_month"))

#calculating the monthly average (by averaging over stations).
monthly_average_precipitation = precipitation_sum_station_month.groupBy("year", "month").agg(F.avg("precipitation_sum_over_month").alias("monthly_avg_precip"))
monthly_average_precipitation = monthly_average_precipitation.withColumn("monthly_avg_precip", F.format_number(F.col("monthly_avg_precip"), 2))
monthly_average_precipitation = monthly_average_precipitation .orderBy("year", "month")  #sort by year and month
monthly_average_precipitation.rdd.saveAsTextFile("BDA/output")


'''
monthly_average_precipitation up to two decimal
Row(year=1994, month=1, monthly_avg_precip=u'22.10')
Row(year=1994, month=2, monthly_avg_precip=u'22.50')
Row(year=1994, month=3, monthly_avg_precip=u'37.60')
Row(year=1994, month=4, monthly_avg_precip=u'23.10')
Row(year=1994, month=5, monthly_avg_precip=u'25.10')
Row(year=1994, month=6, monthly_avg_precip=u'45.10')
Row(year=1994, month=7, monthly_avg_precip=u'0.00')
Row(year=1994, month=8, monthly_avg_precip=u'58.80')
Row(year=1994, month=9, monthly_avg_precip=u'94.60')
Row(year=1994, month=10, monthly_avg_precip=u'33.20')
'''
