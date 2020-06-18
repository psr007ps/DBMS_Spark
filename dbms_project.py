from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import substring
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import timeit
import sys

path_to_location_file = sys.argv[1]
path_to_records_file = sys.argv[2]
path_to_output_file = sys.argv[3]

"""**TASK 1**"""

#take start time
import time
startTime = time.clock()

sc = SparkContext('local')
sqlcontext = SQLContext(sc)
spark = SparkSession(sc)
data_locations = spark.read.option("header", "true").csv(path_to_location_file)

#group data with location
grouped_data_locations = data_locations.filter(data_locations["CTRY"] == "US").groupBy("STATE")
grouped_data_locations.count().show(60)

"""**Task 2**"""

#function to parse and calculate value of precipitation
def calculatePrecipitation(data):
    data = data.split()
    stn = data[0:1]
    year = data[2:3] 
    prcp = data[19:20]
    temp1 = prcp[0][-1]
    temp2 = float(prcp[0][:-1])

    if temp1 == 'A':
        temp2 = temp2 * 4
    elif temp1 == 'B':
        temp2 = temp2 * 2
    elif temp1 == 'C':
        temp2 = temp2 * 1.33
    elif temp1 == 'E':
        temp2 = temp2 * 2

    year = stn + year
    year.append(str(temp2))
    return (year)

recordData = sc.textFile("file:" + str(path_to_records_file) + "/*.txt")
header = recordData.first()
fields = [StructField("STN---", StringType(), True), StructField("YEARMODA", StringType(), True), StructField("PRCP", StringType(), True)]
schema = StructType(fields)

#Remove extra headers
recordData = recordData.filter(lambda row:row != header)

#Remove rows with 99.99 before going to map
recordData = recordData.filter(lambda row:row.split()[19][:-1] != "99.99")

#Creating map
calcPRCP = recordData.map(lambda k: calculatePrecipitation(k))

#Calculate the value of precipitation
df = spark.createDataFrame(calcPRCP, schema=schema)

#get month from date
getMonth =  udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
df2 = df.withColumn('Month', date_format(getMonth(col('YEARMODA')), 'MMMM'))

#rename column
df2 = df2.withColumnRenamed("STN---", "STN")

#define condition and join df3 with df2
condition = [data_locations["USAF"] == df2["STN"], data_locations["CTRY"] == "US", data_locations["STATE"].isNotNull()]
df3 = data_locations.join(df2, condition).select(data_locations.STATE, df2.STN, df2.PRCP, df2.Month)

#group by month and aggregate by average
df4 = df3.groupBy("STATE", "Month").agg({"PRCP": 'avg'})
df5 = df4.withColumn("Avg_PRCP", col("avg(PRCP)"))

print(recordData.take(20))
df5.show(20)

"""**Task 3**"""

df8 = df5.orderBy('STATE', "Month")
df9_max = df8.groupBy("STATE").agg({"Avg_PRCP": 'max'})
df9_min = df8.groupBy("STATE").agg({"Avg_PRCP": 'min'})

#calculate max and min
maxCondition = [df8.STATE == df9_max.STATE, df8.Avg_PRCP == df9_max["max(Avg_PRCP)"]]
minCondition = [df8.STATE == df9_min.STATE, df8.Avg_PRCP == df9_min["min(Avg_PRCP)"]]

df10_max = df8.join(df9_max, maxCondition).select(df9_max.STATE, df8.Avg_PRCP, df8.Month)
df10_min = df8.join(df9_min, minCondition).select(df9_min.STATE, df8.Avg_PRCP, df8.Month)

df10_max = df10_max.withColumnRenamed("Avg_PRCP", "PRCP_Max")
df10_max = df10_max.withColumnRenamed("Month", "PRCP_Max_Month")

df10_min = df10_min.withColumnRenamed("Avg_PRCP", "PRCP_Min")
df10_min = df10_min.withColumnRenamed("Month", "PRCP_Min_Month")


df11 = df10_max.join(df10_min, df10_max["STATE"] == df10_min["STATE"]).select(df10_max["STATE"], df10_max["PRCP_Max"], df10_max["PRCP_Max_Month"], df10_min["PRCP_Min"], df10_min["PRCP_Min_Month"])
df11 = df11.orderBy('STATE')

df11.show(20)

"""**Task 4**"""

df12 = df11.withColumn("Diff", col("PRCP_Max") - col("PRCP_Min"))
df13 = df12.orderBy('Diff')

#take stop time
stopTime = time.clock()

#print total time
print('Total Time to run the code: ', stopTime - startTime)

df13.show(20)

#write output to file
df13.coalesce(1).write.format("csv").option("header", "true").save(str(path_to_output_file) + "/Result")

