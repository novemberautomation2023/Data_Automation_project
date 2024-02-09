import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode

#Spark session creation
spark = SparkSession.builder \
    .master("local") \
    .getOrCreate()

#Reading source1

file= spark.read.option("header", True).csv(r'D:\learning\Python\Shrinivas Project\Data_Automation_project\Source_files\Contact_info.csv')
#file2 = read_data("csv",'/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/Contact_info.csv',spark)

# file.write.mode("overwrite") \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("dbtable", "contact_info_raw") \
#     .option("user", "postgres") \
#     .option("password", "Dharmavaram1@") \
#     .option("driver", 'org.postgresql.Driver') \
#     .save()


#file3 = file.union(file2)

file.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/XE") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "ETL_SHREENI_PROJ.contact_info_raw") \
    .option("user", "system") \
    .option("password", "shreyas") \
    .save()

file.createOrReplaceTempView("file")

contact_info_bronze = spark.sql(
    """ select
    cast(Identifier as decimal(10)) Identifier,
    upper(Surname) Surname,
    upper(given_name) given_name,
    upper(middle_initial) middle_initial,
    suffix,
    Primary_street_number,
    primary_street_name,
    city,
    state,
    cast(zipcode as decimal(10)) zipcode,
    Primary_street_number_prev,
    primary_street_name_prev,
    city_prev,
    state_prev,
    zipcode_prev,
    Email,
    translate(Phone,'+-','') phone,
    rpad(birthmonth,8,'0') birthmonth
    from file
    """
)
print("insert bronze")
contact_info_bronze.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/XE") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "ETL_SHREENI_PROJ.contact_info_bronze") \
    .option("user", "system") \
    .option("password", "shreyas") \
    .save()
print("insert bronze success")

contact_info_bronze.createOrReplaceTempView("contact_info_bronze")
contact_info_bronze.show()

contact_info_silver= spark.sql(
        """
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number,
        primary_street_name,
        city,
        state,
        zipcode,
        Email,
        Phone,
        birthmonth,
        'Y' as Current_ind
        from contact_info_bronze
        union
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number_prev,
        primary_street_name_prev,
        city_prev,
        state_prev,
        zipcode_prev,
        Email,
        Phone,
        birthmonth,
        'N' as Current_ind
        from contact_info_bronze
        """)

contact_info_silver.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/XE") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .option("dbtable", "ETL_SHREENI_PROJ.contact_info_silver") \
    .option("user", "system") \
    .option("password", "shreyas") \
    .save()
