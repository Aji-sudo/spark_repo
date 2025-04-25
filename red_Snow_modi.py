from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("RedshiftToSnowflake_Migration") \
    .config("spark.driver.extraJavaOptions", 
        "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-exports java.base/sun.util.calendar=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", 
        "--add-exports java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-exports java.base/sun.util.calendar=ALL-UNNAMED") \
    .getOrCreate()

redshift_url =  "jdbc:redshift://nabuswat-redshift-cluster-1.cj3dic2hrmrh.us-east-2.redshift.amazonaws.com:5439/nabuswat"
redshift_options = {
    "user": "nabuswat",
    "password": "BEnAYr54mHVy",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

df_redshift = spark.read.jdbc(url=redshift_url, table="public.combined_table_trim", properties=redshift_options)

# Repartition the DataFrame for parallel processing (depending on your data size, adjust the number of partitions)
df_redshift_repartitioned = df_redshift.repartition(100)  # Increase this number based on the available cores

#redshift_df = spark.read.format("jdbc").options(**redshift_options).load()

# --- Snowflake Connection Props ---
sf_options = {
    "sfURL": "su11785.ap-south-1.aws.snowflakecomputing.com",
    "sfUser": "modaksnowflake",
    "sfPassword": "2tF8NPXsL6Cbb8nQ",
    "sfDatabase": "test",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "dbtable": "target_table_Ag2802_156"
}

# --- Write to Snowflake ---
df_redshift_repartitioned .write.format("snowflake") \
    .options(**sf_options) \
    .mode("overwrite") \
    .save()
