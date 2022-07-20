import json
import os
from os import walk
import sys

from Helper_Code.helper_variables import *
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def get_spark_session(app_name: str, conf: SparkConf):
    """
    Begins a spark session for pyspark
        app_name: str of spark session name
        conf: SparkConf

    """
    with open("/home/jupyter/config/spark-defaults.json") as f:
        config = json.load(f)
    for key, value in config.items():
        conf.set(key, value)
    conf.setMaster("k8s://https://kubernetes.default.svc.cluster.local")
    conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()


def merge_files(path, spark, show=True):
    """
    Each folder of data contains multiple files.
    Combines all files into one pyspark dataframe
        path: path to folder with parquet files that need to be merged
        spark: spark session
        show: default True if final preview of df is desired
    """
    filenames = next(walk(path), (None, None, []))[2]
    df = None
    for file in filenames:
        if file[-7:] == "parquet":
            if df != None:
                df = df.union(spark.read.parquet(path + "/" + file))
            else:
                df = spark.read.parquet(path + "/" + file)
    print((df.count(), len(df.columns)))
    if show:
        df.show()
    return df


def merge_dfs(column, path, spark, merge_type="outer", dfs=[]):
    """
    Combines multiple dataframes on a given shared column
        column: shared column for files to be combined on
            will find all possible dataframes in the entirety of
            OMOP data that contain that column
        path: path to main folder of OMOP data
        spark: spark session
        merge_type: "outer", "left", "right"
        dfs: list of strings of dataframes that should be combinded on column.
            Used if not all dfs with column are wanted.
    """
    if dfs == []:
        for df in df_to_columns:
            if column in df_to_columns[df]:
                dfs.append(df)
    print("Dataframes to Merge: ", dfs)
    print("Number of Dataframes: ", len(dfs))
    if len(dfs) >= 1:
        print("Merging: ", dfs[0])
        summary_df = merge_files(path + dfs[0] + "/", spark)
        for df in dfs[1:]:
            print("Merging: ", df)
            summary_df = summary_df.join(
                merge_files(path + df + "/", spark), on=column, how=merge_type
            )
    else:
        scheme = StructType([StructField(column, StringType(), True)])
        emptyRDD = spark.sparkContext.emptyRDD()
        return spark.createDataFrame(data=emptyRDD, schema=scheme)
    return summary_df
