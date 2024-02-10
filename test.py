from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()
dataList = [("Java", 20000), ("Python", 10000), ("Scala", 30000)]

df = spark.createDataFrame(dataList, schema=["Language", "Fee"])
df.show()
