
import json
from pyspark.sql import SparkSession
import pandas as pd
import json
from pyspark.sql.functions import collect_set
from pyspark.sql import SparkSession
import pandas as pd
import json

from pyspark.sql.types import StructType


spark = SparkSession.builder.master("local").appName("test_execution")\
     .config("spark.jars", "/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .config("spark.driver.extraClassPath", "/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .config("spark.executor.extraClassPath","/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .getOrCreate()

df = spark.createDataFrame([1,2],('id','id1'))

df.show()

source = spark.read.option("header", True).csv('/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/Contact_info.csv')

source.printSchema()

with open("/Users/harish/PycharmProjects/Data_Automation_project/schem/contact_info_schema.json", 'r') as f:
    schema = StructType.fromJson(json.load(f))

print(schema)

source_with_ext_schema = spark.read.schema(schema).option("header", True).csv('/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/Contact_info.csv')
source_with_ext_schema.printSchema()