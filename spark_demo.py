from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('demo spark')\
        .master("local[2]")\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
rdd =spark.sparkContext.parallelize([10,20,30,40])
print(rdd.collect())
