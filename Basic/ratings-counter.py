from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import collections
import datetime

spark = SparkSession\
        .builder\
        .appName("PythonRatings")\
        .getOrCreate()

print("-------------------------------------#Program Started--------------------------------------")
now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M:%S"))
df = spark.read.csv(r"D:\dataset\ml-20m\movies.csv")
#ratings = lines.map(lambda x: x.split(",")[2])
#result = lines.countByValue()
data=df.sort(col("_c0").desc()).collect()

for key in data:
        print(key[0]+" "+key[1]+" "+key[2])

print("-------------------------------------#Program Stopped--------------------------------------")
now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M:%S"))