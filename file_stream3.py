from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("CSVtoCSVSink") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])


input_path = "c:/data/input/"

df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(input_path)
filter = df.filter(col("salary") > 20000)
query = filter.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "c:/data/csv_output/") \
    .option("checkpointLocation", "c:/data/csv_output/check") \
    .option("header", "true") \
    .start()

query.awaitTermination()
