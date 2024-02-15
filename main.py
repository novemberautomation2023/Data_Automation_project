"""This file will be starting point for automation execution"""

from Utility.files_read_lib import *
from Utility.validation_lib import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
import datetime

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#print(batch_id)


os.environ.setdefault("project_path", os.getcwd())
project_path = os.environ.get("project_path")

# jar_path = pkg_resources.resource_filename('jars', 'postgresql-42.2.5.jar')
jar_path = project_path + "/jars/postgresql-42.2.5.jar"
print(jar_path)
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()
# template_path = pkg_resources.resource_filename("Config", "Master_Test_Template.xlsx")
template_path = project_path + '/Config/Master_Test_Template.xlsx'
print(template_path)
Test_cases = pd.read_excel(template_path)
run_test_case = Test_cases.loc[(Test_cases.execution_ind == 'Y')]
# print(run_test_case)
# print(run_test_case.columns)
df = spark.createDataFrame(run_test_case)
df.show()

validations = df.groupBy('source', 'source_type',
                         'source_db_name', 'schema_path', 'source_transformation_query_path', 'target',
                         'target_type', 'target_db_name', 'target_transformation_query_path',
                         'key_col_list', 'null_col_list', 'unique_col_list').agg(
    collect_set('validation_Type').alias('validation_Type'))

validations = validations.withColumn('batch_id',lit(batch_id))

validations.show(truncate=False)
#
validations = validations.collect()

Out = {"batch_id": [],
       "validation_Type": [],
       "Source_name": [],
       "target_name": [],
       "Number_of_source_Records": [],
       "Number_of_target_Records": [],
       "Number_of_failed_Records": [],
       "column": [],
       "Status": [],
       }
#
schema = ["batch_id",
          "validation_Type",
          "Source_name",
          "target_name",
          "Number_of_source_Records",
          "Number_of_target_Records",
          "Number_of_failed_Records",
          "column",
          "Status"]


for row in validations:
    print("*" * 80)
    print(row)
    print("Execution started for dataset ".center(80))
    print("*" * 80)

    if row['source_type'] == 'table':
        source = read_data(row['source_type'], row['source'], spark=spark, database=row['source_db_name'],
                               sql_path=row['source_transformation_query_path'])
    else:
        source_path = pkg_resources.resource_filename('source_files', row['source'])
        print("Source_path", source_path)
        source = read_data(row['source_type'], source_path, spark, schema=row['schema_path'])

    if row['target_type'] == 'table':
        print(row['target_type'], row['target'], row['target_db_name'])
        target = read_data(row['target_type'], row['target'], spark=spark, database=row['target_db_name'],
                               sql_path=row['target_transformation_query_path'])
    else:
        target_path = pkg_resources.resource_filename('source_files', row['target'])
        target = read_data(row['target_type'], target_path, spark)

    source.show(n=2)
    target.show(n=2)
    for validation in row['validation_Type']:

        print(validation)
        if validation.strip().lower() == 'count_check':
            count_check(source, target, Out, row)
        elif validation == 'duplicate_check':
            duplicate_check(target, row['key_col_list'], Out, row)
        elif validation == 'null_value_check':
            null_value_check(target, row['null_col_list'], Out, row)
        elif validation == 'uniqueness_check':
            uniqueness_check(target, row['unique_col_list'], Out, row)
        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source, target, row['key_col_list'], Out, row)
        elif validation == 'records_present_only_in_target':
            records_present_only_in_target(source, target, row['key_col_list'], Out, row)
        elif validation == 'data_compare':
            data_compare(source, target, row['key_col_list'], Out,row)


print(Out)
summary = pd.DataFrame(Out)
summary.to_csv("summary.csv")
summary = spark.createDataFrame(summary).withColumn("fail_perce", col('Number_of_failed_Records')/col('Number_of_target_Records'))
df2 = df.select('test_case_id','validation_Type','source','source_type','target','target_type')
df2.show()
summary.show()
df2 = df2.withColumnRenamed("source", "Source_name") \
         .withColumnRenamed("target","target_name")
df2.show()

summary.join(df2, ['validation_Type','Source_name','target_name'], 'inner').show()

