###
### import ratings.csv to MSSQL
###
from pyspark.sql import SparkSession
import collections
import datetime

spark = SparkSession\
        .builder\
        .appName("SQLJDBC")\
        .getOrCreate()

# MSSQL JDBC CONNECTION
def jdbc_mssql_python(spark):
    jdbcHostName="DBServerNameOrIP"
    jdbcDataBase="DataBaseName"
    jdbcPort=1433#TCP/IP PORT NUMBER
    jdbcUserName="Username"
    jdbcPassword="Password"
    jdbcUrl="jdbc:sqlserver://{0}:{1};database={2};".format(jdbcHostName,jdbcPort,jdbcDataBase)
    connectionProperties={
        "user":jdbcUserName,
        "password":jdbcPassword,
        "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    pushdown_query = "ratings"
    df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    list=df.groupBy("rating").count().collect()
    sortedResults= sorted(list)
    
    for key, value in sortedResults:
        print("%s %i" % (key, value))
    
   
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()
    print("-------------------------------------#Program Started--------------------------------------")
    now = datetime.datetime.now()
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    jdbc_mssql_python(spark)
    spark.stop()
    now = datetime.datetime.now()
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    print("-------------------------------------#Program Stoped--------------------------------------")
    
