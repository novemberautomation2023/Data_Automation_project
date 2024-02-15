from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()

# Create a DataFrame
data = [("John", 25, "Engineer"), ("Alice", 30, "Manager"), ("Bob", 35, "Developer")]
df = spark.createDataFrame(data, ["Name", "Age", "Occupation"])

# Rename the columns "Age" and "Occupation" to "Years" and "Job", respectively
new_df = df \
    .withColumnRenamed("Age", "Years") \
    .withColumnRenamed("Occupation", "Job")

# Show the original and new DataFrames
df.show()
new_df.show()