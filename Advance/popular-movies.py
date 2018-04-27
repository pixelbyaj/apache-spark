from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

def getMovies(row):
    movieDetails=row.split(",")
    
    if movieDetails[0]=="userId":
        return (0,1)
    
    return (int(movieDetails[1]),1)

        
lines = sc.textFile("file:///D:/dataset/ml-20m/ratings.csv")
movies = lines.map(getMovies)
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
