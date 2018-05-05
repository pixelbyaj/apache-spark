#Author Neha Bhoi, Abhishek Joshi
#github https://github.com/Nehabhoi/apache-spark
import json

class DBConnection:
    jdbcUrl = {
        "MYSQL":{"url":"jdbc:mysql://${0}:${1}/${2};","driver":"com.mysql.jdbc.Driver"},
        "MSSQL":{"url":"jdbc:sqlserver://{0}:{1};database={2};","driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
        }
    
    def __init__(self,spark,configFilePath=None):
        if configFilePath:
            with open(configFilePath) as dbDetails:
               
                #read json file
                dbDetailsJSON=json.load(dbDetails)
                
                #Get Connection configuration1
                connectionType=dbDetailsJSON["ConnectionType"]
                jdbcUrl=self.jdbcUrl[connectionType]["url"]
                jdbcDriver=self.jdbcUrl[connectionType]["driver"]
               
                #Create the JDBC Url
                self.Url=jdbcUrl.format(dbDetailsJSON['HostName']
                ,dbDetailsJSON['Port'],dbDetailsJSON['DataBaseName'])
                self.ConnectionProperties={ 
                        "user":dbDetailsJSON['UserName'], 
                        "password":dbDetailsJSON['Password'], 
                        "driver":jdbcDriver 
                }
                self.spark=spark
        
    def getDataFrame(self,pushdown_query):
        #Push down a query to the database engine
        df=self.spark.read.jdbc(url=self.Url,table=pushdown_query,properties=self.ConnectionProperties) 
        return df

    def getRddFromDataFrame(self,aList):
        print (isinstance(aList,list))
        if isinstance(aList,list):
            print(len(aList))
            return self.spark.sparkContext.parallelize(aList)


