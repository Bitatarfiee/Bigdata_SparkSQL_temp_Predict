from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)

rdd= sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))

tempReadingsRow = parts.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]),p[2], float(p[3]), p[4] ))

tempReadingsString = ["station", "date", "year", "time", "value","quality"]

# Apply the schema to the RDD.
schemaTempReadings = sqlContext.createDataFrame(tempReadingsRow,tempReadingsString)

# filter data between year 1950 and 2014
temp_filtered = schemaTempReadings.filter((schemaTempReadings["year"] >= 1950) & (schemaTempReadings["year"] <= 2014))

#finding the max temp of each year and sorting it in with descending order
max_grouped_data = temp_filtered.groupBy("year").agg(F.max("value").alias("value"))

# Join with the original data to get the number of stations related to the max temperature.
max_temp_data = max_grouped_data.join(temp_filtered, ['year','value'],'inner')

max_sorted_data= max_temp_data.orderBy(max_temp_data["value"].desc())
tempReadingsmax= max_sorted_data.select("year", "value", "station")

#tempReadings.show()

#finding the min temp of each year and sorting it in with ascending order
min_grouped_data = temp_filtered.groupBy("year").agg(F.min("value").alias("value"))
min_temp_data= min_grouped_data.join(temp_filtered, ['year','value'],'inner')
min_sorted_data= min_temp_data.orderBy(min_temp_data["value"].asc())
tempReadingsmin= min_sorted_data.select("year", "value", "station")

tempReadingsmax.rdd.saveAsTextFile("BDA/output/sorted_data_max")
tempReadingsmin.rdd.saveAsTextFile("BDA/output/sorted_data_min")



#Sorted max of data in each year:
'''
year\Max_temp\station
Row(year=1975, value=36.1, station=u'86200')
Row(year=1992, value=35.4, station=u'63600')
Row(year=1994, value=34.7, station=u'117160')
Row(year=2010, value=34.4, station=u'75250')
Row(year=2014, value=34.4, station=u'96560')
Row(year=1989, value=33.9, station=u'63050')
Row(year=1982, value=33.8, station=u'94050')
Row(year=1968, value=33.7, station=u'137100')
Row(year=1966, value=33.5, station=u'151640')
Row(year=1983, value=33.3, station=u'98210')
Row(year=2002, value=33.3, station=u'78290')
Row(year=2002, value=33.3, station=u'78290')
Row(year=1970, value=33.2, station=u'103080')
Row(year=1986, value=33.2, station=u'76470')

'''

#Sorted min of data in each year:
'''
year\Min_temp\station
Row(year=1966, value=-49.4, station=u'179950')
Row(year=1999, value=-49.0, station=u'192830')
Row(year=1999, value=-49.0, station=u'192830')
Row(year=1978, value=-47.7, station=u'155940')
Row(year=1987, value=-47.3, station=u'123480')
Row(year=1967, value=-45.4, station=u'166870')
Row(year=1980, value=-45.0, station=u'191900')
Row(year=1980, value=-45.0, station=u'191900')
Row(year=1956, value=-45.0, station=u'160790')
Row(year=1971, value=-44.3, station=u'166870')
Row(year=1986, value=-44.2, station=u'167860')
Row(year=1981, value=-44.0, station=u'166870')
Row(year=1981, value=-44.0, station=u'166870')
Row(year=1979, value=-44.0, station=u'112170')

'''
