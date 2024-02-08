# import os
# import sys
# # print(os.getcwd())
# # sys.path.append(os.getcwd())
# #
# #sys.path.clear()
# sys.path.append(os.getcwd())
# #
# print(sys.path[0]+'jars/postgresql-42.2.5.jar')



from Utility.validation_lib import *
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

jar_path = pkg_resources.resource_filename('jars', 'postgresql-42.2.5.jar')
#jar_path = "/Users/harish/PycharmProjects/Data_Automation_project/jars/ojdbc11.jar"
spark = SparkSession.builder.master("local")\
    .appName("test") \
    .config("spark.jars",jar_path)\
    .config("spark.driver.extraClassPath",jar_path) \
    .config("spark.executor.extraClassPath",jar_path) \
    .getOrCreate()
template_path = pkg_resources.resource_filename("Config","Master_Test_Template.xlsx")
#template_path = "/Users/harish/PycharmProjects/Data_Automation_project/Config/Master_Test_Template.xlsx"
Test_cases = pd.read_excel(template_path)
run_test_case = Test_cases.loc[(Test_cases.execution_ind=='Y')]
print(run_test_case)
print(run_test_case.columns)
df = spark.createDataFrame(run_test_case)

validations = df.groupBy('source', 'source_type',
       'source_db_name','schema_path', 'source_transformation_query_path', 'target',
       'target_type', 'target_db_name', 'target_transformation_query_path',
       'key_col_list', 'null_col_list', 'unique_col_list').agg(collect_set('validation_Type').alias('validation_Type'))

validations.show(truncate=False)
#
validations = validations.collect()

# for i in validations:
#     print(i['schema_path'])#,i['source_type'],i['target'],i['target_type'],i['validation_Type'])
#     # for val in  i['validation_Type']:
#     #     print(i['source'],i['source_type'],i['target'],i['target_type'],val)

Out = {"TC_ID":[],
       "test_Case_Name":[],
       "Source_name":[],
       "target_name":[],
       "Number_of_source_Records":[],
       "Number_of_target_Records":[],
       "Number_of_failed_Records":[],
       "column":[],
       "Status":[],
       }
schema= ["TC_ID",
         "test_Case_Name",
         "Source_name",
         "target_name",
         "Number_of_source_Records",
         "Number_of_target_Records",
         "Number_of_failed_Records",
         "column",
         "Status"]
#
#
for row in validations:
    if row['source_type'] == 'table':
        source = read_data(row['source_type'], row['source'], spark=spark, database=row['source_db_name'],sql_path=row['target_transformation_query_path'])
    else:
        source_path = pkg_resources.resource_filename('Source_Files', row['source'])
        print(source_path)
        source = read_data(row['source_type'], source_path, spark,schema=row['schema_path'])

    if row['target_type'] == 'table':
        print(row['target_type'], row['target'], row['target_db_name'])
        target = read_data(row['target_type'], row['target'], spark=spark, database=row['target_db_name'],sql_path=row['target_transformation_query_path'])
    else:
        target_path = pkg_resources.resource_filename('Source_Files', row['target'])
        print(source_path)
        target = read_data(row['target_type'], target_path, spark)

    source.show(n=2)
    target.show(n=2)
    for validation in row['validation_Type']:
        print(validation)
        if validation == 'count_validation':
            count_validation(source, target, Out,row)
        elif validation == 'duplicate':
            duplicate(target,row['key_col_list'], Out,row)
        elif validation == 'Null_value_check':
            Null_value_check(target, row['null_col_list'], Out,row)
        elif validation == 'Uniquess_check':
            Uniquess_check(target, row['unique_col_list'], Out,row)

        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source, target, row['key_col_list'], Out,row)

        elif validation == 'records_present_only_in_target':
            records_present_only_in_target(source, target, row['key_col_list'], Out,row)

        elif validation == 'data_compare':
            data_compare(source, target, row['key_col_list'], Out)



df = pd.DataFrame(Out)

df.to_csv("summary.csv")


spark.createDataFrame(df).show()