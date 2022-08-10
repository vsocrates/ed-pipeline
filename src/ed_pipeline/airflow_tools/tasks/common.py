from typing import Sequence
from airflow.decorators import task
from pyspark import SparkConf

from ed_pipeline.utils.helpful_functions import get_spark_session
from pyspark import sql

from ed_pipeline.utils.helpful_functions import merge_files
from ed_pipeline.utils.helpful_functions import get_spark_session


# import the logging module
import logging

# get the airflow.task logger
task_logger = logging.getLogger('airflow.task')


# @task()
# def start_spark_session(app_name: str):
#     """This is a function that will run within the DAG execution"""
#     spark = get_spark_session(app_name, SparkConf())
#     task_logger.critical('This log shows a critical error!')
#     return spark

@task()
def merge_data_pull(df_list: Sequence[sql.DataFrame], merge_on: str, merge_method: str = "left"):

    merged_out = None
    first_df = df_list[0]
    merged_out = first_df.join(df_list[1], on=merge_on, how=merge_method)
    for df in df_list[2:]:
        merged_out = merged_out.join(df, on=merge_on, how=merge_method)

    return merged_out


    
@task()
def merge_data_by_path(spark_app_name: str, output_path: str, df_list: Sequence[str], merge_on: str, merge_method: str = "left"):
    '''We assume the df_list is a list of paths to parquet files
    '''    
    spark = get_spark_session(spark_app_name, SparkConf())
    
    merged_out = None
    first_df = merge_files(df_list[0], spark, show=False)
    merged_out = first_df.join(merge_files(df_list[1], spark, show=False), on=merge_on, how=merge_method)
    for df_path in df_list[2:]:
        df = merge_files(df_path, spark, show=False)
        merged_out = merged_out.join(df, on=merge_on, how=merge_method)

    merged_out.write.mode("overwrite").parquet(f"{output_path}/{task_id}.parquet")
    # spark.stop()

    # return merged_out