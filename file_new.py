
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("MyFirstSparkApp") \
    .getOrCreate()

# Now you can use the `spark` session to interact with Spark
# For example, read a CSV file
df = spark.read.csv("C:/Users/AG2802/Downloads/employee_data.csv", header = True, inferSchema=True)
schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df.createOrReplaceTempView("employees")
df.printSchema()
result = spark.sql("select * from employees")
result2 = spark.sql("select avg(salary) from employees")
result3 = spark.sql("with avg_sal as (select country, avg(salary) as avg_salary from employees group by country ) select e.id, e.name, e.age, e.country, e.salary, a.avg_salary from employees e join avg_sal a on e.country=a.country")
# result.show()

result3.show()

# Show the dat

# Stop the session
spark.stop()
#result1= spark.sql("select country, avg(age) as average from employees group by country order by avg(age) desc") 
