from pyspark.sql import SparkSession
 
spark = SparkSession.builder \

    .appName("PySpark Sample") \

    .getOrCreate()
 
data = [

    (1, "Alice", 1000),

    (2, "Bob", 1500),

    (3, "Catherine", 2000)

]

columns = ["id", "name", "salary"]
 
df1 = spark.createDataFrame(data, columns)
 
df2 = spark.createDataFrame(data, columns)
 
df = df1.union(df2)
 
df.show()
 
