from pyspark import SparkConf, SparkContext
import collections
import datetime

conf = SparkConf().setMaster("local[*]").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

print("-------------------------------------#Program Started--------------------------------------")
now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M:%S"))
lines = sc.textFile(r"D:\dataset\ml-20m\ratings.csv")
ratings = lines.map(lambda x: x.split(",")[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

print("-------------------------------------#Program Stoped--------------------------------------")
now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M:%S"))