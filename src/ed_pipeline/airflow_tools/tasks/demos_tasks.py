import datetime
from typing import Dict, Mapping, Sequence

from airflow.decorators import dag, task
from ed_pipeline.modules import demos
from ed_pipeline.utils.helpful_functions import get_spark_session
from pyspark import SparkConf, sql
from pyspark.sql import SparkSession

# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively


@task()
def demos_pull_task(
    spark_app_name: str,
    base_url: str,
    output_path: str,
    group_value: str,
    age: Sequence[int] = [],
    gender: Sequence[str] = [],
    race: Sequence[str] = [],
    ethnicity: Sequence[str] = [],
    insurance: Sequence[str] = [],
    visit_date: Sequence[datetime.datetime] = [],
    visit_location: Mapping[str, str] = {},
    care_site: Mapping[str, str] = {},
    rand_sample_size: int = 0,
) -> Dict[str, sql.DataFrame]:
    """This is a function that will run within the DAG execution"""
    spark = get_spark_session(spark_app_name, SparkConf())

    demos_data, comp_data, = demos.demographic_pull(
        spark,
        base_url,
        group_value,
        age,
        gender,
        race,
        ethnicity,
        insurance,
        visit_date,
        visit_location,
        care_site,
        rand_sample_size,
    )

    demos_data.write.mode("overwrite").parquet(f"{output_path}/demos_data.parquet")
    comp_data.write.mode("overwrite").parquet(f"{output_path}/demos_comp_data.parquet")
    # spark.stop()

    # return {"main":demos_data, "comp":comp_data}
