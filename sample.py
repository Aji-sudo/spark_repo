import sys

import logging

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
 
# -----------------------------------

# Logging Configuration

# -----------------------------------

logging.basicConfig(

    level=logging.INFO,

    format='%(asctime)s [%(levelname)s] %(message)s',

    handlers=[logging.StreamHandler(sys.stdout)]

)

logger = logging.getLogger(__name__)
 
# -----------------------------------

# Schema Definition

# -----------------------------------

employee_schema = StructType([

    StructField("id", IntegerType(), nullable=False),

    StructField("name", StringType(), nullable=False),

    StructField("salary", IntegerType(), nullable=False)

])
 
# -----------------------------------

# Spark Job Function

# -----------------------------------

def process_employee_data(spark):

    logger.info("Creating sample employee data...")
 
    data = [

        (1, "Alice", 1000),

        (2, "Bob", 1500),

        (3, "Catherine", 2000)

    ]
 
    # Create two DataFrames with the same data

    df1 = spark.createDataFrame(data, schema=employee_schema)

    df2 = spark.createDataFrame(data, schema=employee_schema)
 
    logger.info("Combining dataframes using union...")

    df_combined = df1.union(df2)
 
    logger.info("Final combined DataFrame:")

    df_combined.show()
 
    return df_combined
 
# -----------------------------------

# Main Entry Point

# -----------------------------------

def main():

    try:

        logger.info("Starting Spark session...")

        spark = SparkSession.builder \

            .appName("Employee Data Processing") \

            .getOrCreate()
 
        df_result = process_employee_data(spark)

        logger.info("Data processing completed successfully.")
 
    except Exception as e:

        logger.error("An error occurred during Spark processing.", exc_info=True)

        sys.exit(1)
 
    finally:

        logger.info("Stopping Spark session.")

        spark.stop()
 
# -----------------------------------

# Run Script

# -----------------------------------

if __name__ == "__main__":

    main()

 
