from typing import Sequence
from airflow.decorators import task
from pyspark import SparkConf

from ed_pipeline.utils.helpful_functions import get_spark_session
from pyspark import sql

@task()
def start_spark_session(app_name: str):
    """This is a function that will run within the DAG execution"""
    spark = get_spark_session(app_name, SparkConf())
    return spark

@task()
def merge_data_pull(df_list: Sequence[sql.DataFrame], merge_on: str, merge_method: str = "left"):

    merged_out = None
    first_df = df_list[0]
    merged_out = first_df.join(df_list[1], on=merge_on, how=merge_method)
    for df in df_list[2:]:
        merged_out = merged_out.join(df, on=merge_on, how=merge_method)

    return merged_out