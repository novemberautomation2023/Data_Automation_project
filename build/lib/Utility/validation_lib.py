#Schema/DDL
#count
#duplicate
#transformation
#column value validtaion
#Null value check
#Uniqueness check
#incremental --> SCD types, CDC
import json

from Utility.write_db_file_lib import write_output, Out
import pandas as pd
from pyspark.sql.types import StructType

from pyspark.sql.functions import *

# Source csv --> raw --> bronze -- Silver
# D1 -            100      - 100       -100
# D2(50,25e,25n)  150      - 125       -125( hash key)
# D3(3,1em,1n,1nc)153      - 127       -127( hash key)

def count_validation(sourcedf, targetdf,Out):
    source_cnt= sourcedf.count()
    target_cnt = targetdf.count()
    diff = source_cnt - target_cnt
    if source_cnt == target_cnt:
        print("count is matching between source and target")
        write_output(1,"count_validation",source_cnt,
                     target_cnt,'pass',0,Out)
    else:
        print("Count is not matching and difference is", abs(source_cnt-target_cnt))
        write_output(1, "count_validation", source_cnt,
                     target_cnt, 'Fail',
                     abs(diff), Out)


def duplicate(dataframe, key_column : list,Out):
    dup_df = dataframe.groupBy(key_column).count().filter('count>1')
    target_count = dataframe.count()
    if dup_df.count()>0:
        print("Duplicates present")
        dup_df.show(10)
        write_output(2, "duplicate", "NA",
                     target_count, "Fail", dup_df.count(), Out)
    else:
        print("No duplicates")
        write_output(2, "duplicate",
                     "NA", target_count, "pass", 0, Out)


def Uniquess_check(dataframe, unique_column : list,Out):
    target_count = dataframe.count()
    for column in unique_column:
        dup_df = dataframe.groupBy(column).count().filter('count>1')
        if dup_df.count()>0:
            print(f"{column} columns has duplicate")
            dup_df.show(10)
            write_output(3, "Uniqueness", "NA", target_count, "Fail", dup_df.count(), Out)
        else:
            print("All records has unique records")
            write_output(3, "Uniqueness", "NA", target_count, "Pass", 0, Out)

def records_present_only_in_target(source,target,keyList:list,Out):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("SourceCount is null or SourceCount != TargetCount ").count()
    print("Key column record present in target but not in Source :" + str(count))
    source_count =source.count()
    target_count = target.count()
    if count > 0:
        count_compare.filter("SourceCount is null").show()
        write_output(5, "records_present_only_in_target", source_count, target_count, "fail", count, Out)
    else:
        print("No extra records present in source")
        write_output(5, "records_present_only_in_target", source_count, target_count, "Pass", 0, Out)

def records_present_only_in_source(source,target,keyList,Out):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("TargetCount is null or SourceCount != TargetCount").count()
    source_count = source.count()
    target_count = target.count()
    print("Key column record present in Source but not in target :" + str(count))
    if count > 0:
        count_compare.filter("TargetCount is null").show()
        write_output(6, "records_present_only_in_source", source_count, target_count, "fail",
                     source_count - target_count, Out)

    else:
        print("No extra records present")
        write_output(6, "records_present_only_in_target", source_count, target_count, "Pass", 0, Out)




def schema_check(source_df,target_df,  Out, expected_schema=None):
    if expected_schema is None:
        ls = []
        for scol in source_df.schema.fields:
            for tcol in target_df.schema.fields:
                if scol.name == tcol.name:
                    if scol.dataType == tcol.dataType:
                        pass
                    else:
                        # print("datatype not matching", scol.name, scol.dataType, tcol.dataType)
                        ls.append((scol.name, scol.dataType, tcol.dataType))

        pd.DataFrame(ls,columns=['column_name','sourcedtype','targetdtype'])
        if len(ls)>0:
            write_output(7, "schema_check", "NA", "NA", "fail", len(ls), Out)
        else:
            write_output(7, "schema_check", "NA", "NA", "pass", 0, Out)

    else:
        ls = []
        with open(expected_schema) as file:
            Sourceschema = StructType.fromJson(json.load(file))
        for scol in Sourceschema:
            for tcol in target_df.fields:
                if scol.name == tcol.name:
                    if scol.dataType == tcol.dataType:
                        pass
                    else:
                        # print("datatype not matching", scol.name, scol.dataType, tcol.dataType)
                        ls.append((scol.name, scol.dataType, tcol.dataType))
            pd.DataFrame(ls, columns=['column_name', 'sourcedtype', 'targetdtype'])


def Null_value_check(dataframe, Null_columns,Out):
    target_count = dataframe.count()
    #col1, col2 ==> ['col1', 'col2']
    Null_columns = Null_columns.split(",")
    for column in Null_columns:
        Null_df = dataframe.select(count(when(col(column).contains('None') | \
                                        col(column).contains('NULL') | \
                                        col(column).contains('Null') | \
                                        (col(column) == '') | \
                                        col(column).isNull() | \
                                        isnan(column), column
                                        )).alias("Null_value_count"))
        cnt = Null_df.collect()
        print(cnt)

        if cnt[0]['Null_value_count']>0:
            print(f"{column} columns has Null values")
            Null_df.show(10)
            write_output(4, "Null_value_check", "NA", target_count, "fail", cnt[0][0], Out)
        else:
            print("No null records present")
            write_output(4, "Null_value_check", "NA", target_count, "pass", 0, Out)

def data_compare( source, target,keycolumn,Out):
    default_exclude_columns = ['create_date', 'update_date','file_name', 'file_update_date','batch_date']
    #source= source.select(except(default_exclude_columns))
    keycolumn = keycolumn.split(",")
    columnList = source.columns
    for column in columnList:
        if column not in keycolumn:
            keycolumn.append(column)
            temp_source= source.select(keycolumn).withColumnRenamed(column,"source_"+column)
            temp_target= target.select(keycolumn).withColumnRenamed(column,"target_"+column)
            keycolumn.remove(column)
            temp_join = temp_source.join(temp_target,keycolumn,how='full_outer')
            failed_records= temp_join.withColumn("comparison", when(col('source_'+column) == col("target_"+column),\
                                                    "True" ).otherwise("False")).filter("comparison == False")
            if failed_records.count()>0:
                failed_records.show(5)

    # SMT = source.exceptAll(target).count()
    # TMS = target.exceptAll(source).count()

from pyspark.sql.functions import length
def zip_check(target, column, spark):
    target.createOrReplaceTempView("target")
    return spark.sql(f"""select * from target where length({column})!=6   """)