from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)

rdd= sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))

tempReadingsRow = parts.map(lambda p: (int(p[1].split("-")[0]),int(p[1].split("-")[1]),int(p[1].split("-")[2]),p[0],float(p[3])))

tempReadingsString = ["year","Month","day", "station_number","value"]

# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow,tempReadingsString)
#schemaTempReadings.show()
# filter data between year 1960 and 2014
temp_filtered = schemaTempReadings.filter((schemaTempReadings["year"] >= 1960) & (schemaTempReadings["year"] <= 2014))

#finding min and max of temp in each year, month, day,station number
max_min_temperatures_daily = temp_filtered.groupBy("year", "month", "day", "station_number").agg(F.max("value").alias("max_temp"), F.min("value").alias("min_temp"))

# average temp in each (year,month,station number) eliminate up to two decimal and sort by year and month
monthly_average = max_min_temperatures_daily.groupBy("year", "month", "station_number").agg(F.avg((F.col("max_temp") + F.col("min_temp")) / 2).alias("avg_temp"))
monthly_average = monthly_average.withColumn("avg_temp", F.format_number(F.col("avg_temp"), 2))
monthly_average = monthly_average.orderBy("year", "month")  #sort by year and month
#monthly_average .show()
monthly_average.rdd.saveAsTextFile("BDA/output")


'''
year_Month_station number_avg_temp
==> part-00117 <==
Row(year=1991, month=8, station_number=u'161790', avg_temp=u'16.77')
Row(year=1991, month=8, station_number=u'82230', avg_temp=u'17.00')
Row(year=1991, month=8, station_number=u'169880', avg_temp=u'13.51')
Row(year=1991, month=8, station_number=u'98510', avg_temp=u'16.84')
Row(year=1991, month=8, station_number=u'117160', avg_temp=u'16.07')
Row(year=1991, month=8, station_number=u'83130', avg_temp=u'17.18')
Row(year=1991, month=8, station_number=u'114360', avg_temp=u'13.61')
Row(year=1991, month=8, station_number=u'78300', avg_temp=u'17.17')
Row(year=1991, month=8, station_number=u'151290', avg_temp=u'15.59')
Row(year=1991, month=8, station_number=u'65450', avg_temp=u'17.38')
'''
