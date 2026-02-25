from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import sum as _sum

spark = SparkSession.builder.appName("FileStreamAggExample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])

input_path = "c:/data/input/"

df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(input_path)


filtered_df = df.filter("salary > 20000")

query = filtered_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()