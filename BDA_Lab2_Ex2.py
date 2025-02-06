from pyspark import SparkContext, sql
from pyspark.sql import SQLContext, Row

### PART 1: ALL OBSERVATIONS ###

# Set up SparkContext object
sc = SparkContext(appName = 'Lab2_Ex2')

# Upload data file
temps = sc.textFile('BDA/input/temperature-readings.csv')

# Split each line into a list of strings
obs = temps.map(lambda line: line.split(";"))

# Create tuple with year as key and temperature as value
month_year_temps = obs.map(lambda x: (x[1][0:7], float(x[3])))

# Filter out observations before 1950, after 2014, or with temperatures below 10
month_year_temps = month_year_temps.filter(lambda x: int(x[0][0:4]) >= 1950 and int(x[0][0:4]) <= 2014 and x[1] >= 10)

# Set up SQLContext object and data frame
sqlc = SQLContext(sc)
df = sqlc.createDataFrame(month_year_temps, ["month", "temperature"])

# Group observations by month and return the count for each
month_year_counts = df.select("month")
month_year_counts = month_year_counts.groupby("month").count()

# Export the output in a csv file
month_year_counts.write.options(header='True', delimiter=';').csv('BDA/output/BDA_Lab2_Ex2_Output')


### PART 2: INDIVIDUAL STATIONS ####

# Set up SparkContext object
sc = SparkContext(appName = 'Lab2_Ex2')

# Upload data file
temps = sc.textFile('BDA/input/temperature-readings.csv')

# Split each line into a list of strings
obs = temps.map(lambda line: line.split(";"))
obs_row = obs.map(lambda x: Row(station = x[0], year = int(x[1][0:4]), month = int(x[1][5:7]), temperature = float(x[3])))

# Set up SQLContext object and data frame
sqlc = SQLContext(sc)
df = sqlc.createDataFrame(obs_row)

# Filter data set
df = df.filter("year >= 1950").filter("year <= 2014").filter("temperature >= 10")
df = df.select("station", "month", "year").distinct()

# Group observations by month and return the count for each
month_year_counts = df.select("month", "year")
month_year_counts = month_year_counts.groupby("month", "year").count()

# Export the output in a csv file
month_year_counts.write.options(header='True', delimiter=';').csv('BDA/output/BDA_Lab2_Ex2_Output')


### OUTPUT ###
"""
Part 1 Output (All Observations) - First 20 Rows
------------------------------------------------
month;count
1957-03;260
1961-07;39821
1999-10;19156
2013-05;83128
1966-07;56062
1961-05;18010
1994-12;122
2009-07;133570
1979-05;27842
1981-07;59911
1955-12;32
1957-11;486
1988-06;64141
1993-06;40091
1975-06;50205
1999-11;4872
1967-11;508
1987-08;59568
1954-01;4
1987-12;7
"""

"""
Part 2 Output (Individual Stations) - First 20 Rows
---------------------------------------------------
month;year;count
8;1962;301
10;1992;272
6;1994;303
11;1958;16
5;2003;321
4;1960;100
8;2013;300
5;1969;338
4;1989;274
6;1964;303
3;1954;36
6;1960;127
4;1976;301
10;1956;106
4;1962;274
11;2006;147
12;2013;13
7;1954;119
10;1975;341
12;1953;62
"""