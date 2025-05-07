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

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .appName("S3 to Snowflake Data Move") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:3.1.1,net.snowflake:snowflake-jdbc:3.13.6") \
    .getOrCreate()

# Step 2: Define S3 input and Snowflake output paths
input_path = "s3a://j-and-j-test-bucket/target_combined_new/part-00000-ce9be0cc-25d3-470b-9745-19c501275b30-c000.snappy.parquet"
output_table = "s3_snowflake_trial_table"  # The target table in Snowflake

# Step 3: Read data from S3 (assuming Parquet format, can change to CSV, JSON, etc.)
df = spark.read.parquet(input_path)

# Step 4: Set up Snowflake options
sfOptions = {
    "sfURL": "tn30041.europe-west2.gcp.snowflakecomputing.com",
    "sfUser": "ACME_ADMIN",
    "sfPassword": "Ajitha_February@2003",  
    "sfDatabase": "util_db",
    "sfSchema": "public",
    "sfWarehouse": "acme_wh"
}

# Step 5: Write data to Snowflake
df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", output_table) \
    .mode("overwrite") \ 
    .save()

# Step 6: Stop the Spark session
spark.stop()
