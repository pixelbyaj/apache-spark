###
### import ratings.csv to MSSQL
###
from pyspark.sql import SparkSession

import collections
import datetime
from DBConnection import DBConnection

        
class SQLExample:
# Get JDBC CONNECTION
    def getDBConnection(self,spark):
        self.db = DBConnection(spark,"DBJson.json")

    def getDataFrame(self,pushdown_query):
        df = self.db.getDataFrame(pushdown_query)
        list=df.groupBy("rating").count().collect()
        sortedResults= sorted(list)
        rd = self.db.getRddFromDataFrame(sortedResults)
        rating = rd.map(lambda x: x).collect()
        return rating

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()
    print("-------------------------------------#Program Started--------------------------------------")
    now = datetime.datetime.now()
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    sql = SQLExample() 
    sql.getDBConnection(spark)
    ratings = sql.getDataFrame("ratings")
    for key,value in ratings:
         print("%s %i" % (key, value))

    spark.stop()
    now = datetime.datetime.now()

    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    print("-------------------------------------#Program Stoped--------------------------------------")

