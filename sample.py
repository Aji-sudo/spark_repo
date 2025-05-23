# from pyspark.sql import SparkSession
 
# spark = SparkSession.builder \
#     .appName("S3 to S3 Data Move") \
#     .getOrCreate()
 
# input_path = "s3a://j-and-j-test-bucket/target_combined_new/part-00000-ce9be0cc-25d3-470b-9745-19c501275b30-c000.snappy.parquet"
# output_path = "s3a://j-and-j-test-bucket/target_combined_new_up"
 
# df = spark.read.parquet(input_path)
 
# df.write.mode("overwrite").parquet(output_path)
 
# spark.stop()
from pyspark.sql import SparkSession
 
spark = SparkSession.builder \
    .appName("S3 to S3 Data Move") \
    .getOrCreate()
 
input_path = "s3a://j-and-j-test-bucket/folder1.parquet/*.parquet"
output_path = "s3a://j-and-j-test-bucket/target_combined_new_up1/"
 
df = spark.read.parquet(input_path)
 
df.coalesce(1).write.mode("overwrite").parquet(output_path)
 
spark.stop()
