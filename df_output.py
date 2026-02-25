from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("CSVtoCSVSink") \
    .getOrCreate()

df = spark.read.csv('/conent/sample_data/output/',header=True)
print(df.show())