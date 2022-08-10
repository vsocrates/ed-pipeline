import datetime
from typing import Dict, List, Mapping, Sequence

from airflow.decorators import dag, task
from ed_pipeline.modules import vitals
from ed_pipeline.utils.helpful_functions import get_spark_session
from pyspark import SparkConf, sql
from pyspark.sql import SparkSession


@task()
def vitals_pull_task(
    spark_app_name: str,
    base_url: str,
    output_path: str,
    vitals_codes: List[int] = [
        3025315,
        3012888,
        3032652,
        3027018,
        3027598,
        3024171,
        3004249,
        3026258,
        3020891,
    ],
    rand_sample_size: int = 0,
    merge_with: sql.DataFrame = None,
) -> Dict[str, sql.DataFrame]:
    """This is a function that will run within the DAG execution"""

    spark = get_spark_session(spark_app_name, SparkConf())

    (
        vitals_data,
        comp_data,
    ) = vitals.ed_vitals_pull(spark, base_url, vitals_codes, rand_sample_size)
    vitals_data.write.mode("overwrite").parquet(
        f"{output_path}/vitals_data.parquet")
    comp_data.write.mode("overwrite").parquet(
        f"{output_path}/vitals_comp_data.parquet")
    # spark.stop()

    # return {"main":vitals_data, "comp":comp_data}
