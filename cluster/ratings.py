from pyspark.sql import SparkSession
import collections
import datetime

spark = SparkSession\
        .builder\
        .appName("PythonRatings")\
        .getOrCreate()

print("-------------------------------------#Program Started--------------------------------------")
now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M:%S"))
df = spark.read.csv("wasbs://storagebyaj.file.core.windows.net/hdinsght/sparkdataset/ml-20m/ratings.csv")

list=df.groupBy("rating").count().collect()
sortedResults= sorted(list)
    
for key, value in sortedResults:
    print("%s %i" % (key, value))

print("-------------------------------------#Program Stoped--------------------------------------")
now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M:%S"))