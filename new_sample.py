# from pyspark.sql import SparkSession
# spark = SparkSession.builder \
#     .appName("S3 to S3 Data Move") \
#     .getOrCreate()
# input_path = "s3a://j-and-j-test-bucket/target_combined_new/part-00000-ce9be0cc-25d3-470b-9745-19c501275b30-c000.snappy.parquet"
# output_path = "s3a://j-and-j-test-bucket/target_combined_new_up"
# df = spark.read.parquet(input_path)
# df.write.mode("overwrite").parquet(output_path)
# spark.stop()



 
import os
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, current_timestamp
from datetime import datetime


# Current timestamp
time_1 = datetime.now()
print("--------------------------------execution started -------------------------------------")
print("Current Timestamp:", time_1)

connection_parameters = {
 "account": "yu14488.ap-south-1.aws",
    "user": "modaksnowflake",
    "password": "2tF8NPXsL6Cbb8nQ",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "test",
    "schema": "JJ_POC"
}


session = Session.builder.configs(connection_parameters).create()


session.sql("""CREATE or replace stage j_and_j_test_bucket_2
URL = 's3://j-and-j-test-bucket/'
CREDENTIALS = (
  AWS_KEY_ID = 'AKIA2HNV7Z7OH37U54MX'
  AWS_SECRET_KEY = 'P//wv8yjhCDBxMs5TVL266uuty/MUMZS9V6MVW6V'
)
FILE_FORMAT = (TYPE = 'CSV'); """).collect()             

file_format_sql = """
CREATE OR REPLACE FILE FORMAT parquet_2901_1
  TYPE = 'PARQUET';
"""



check_table_sql = """
SELECT COUNT(0) AS table_exists
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'test_2901_python_4' ;
"""


session.sql(file_format_sql).collect()
print("File format created ")
try:
    result = session.sql(check_table_sql).collect()
    print(result)
    if result[0]["TABLE_EXISTS"] == 0:
        create_table_sql = """
        CREATE TABLE test_2901_python_4
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '@j_and_j_test_bucket/target_combined_new_up1/_temporary/0/task_202505081040334183233807577846647_0001_m_000000/part-00000-518fdb0e-c6e0-4366-962b-ce16ee3268fd-c000.snappy.parquet
',
                    FILE_FORMAT => 'parquet_2901_1'
                )
            )
        );
        """
        session.sql(create_table_sql).collect()
        print("Table created from inferred Parquet schema.")
    else:
        print("Table already exists. Skipping creation.")



except SnowparkSQLException as e:
    print("Snowflake SQL execution failed.")
    print(f"Error message: {e}")



except Exception as ex:
    print("An unexpected error occurred.")
    print(f"Error: {ex}")


copy_sql = """
COPY INTO test_2901_python_4
FROM @j_and_j_test_bucket/target_combined_new_up1/_temporary/0/task_202505081040334183233807577846647_0001_m_000000/part-00000-518fdb0e-c6e0-4366-962b-ce16ee3268fd-c000.snappy.parquet
FILE_FORMAT = parquet_2901_1
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
"""
# print("------------------------------------------------copy started using copy into--------------------------------------------")
# now = datetime.now()
# print("Current Timestamp:", now)



session.sql(copy_sql).collect()



print("------------------------------copy into executed----------------------------")
# 
# now = datetime.now()
# print("Current Timestamp:", now)
