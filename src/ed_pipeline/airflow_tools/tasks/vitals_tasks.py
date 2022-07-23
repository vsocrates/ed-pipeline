import datetime
from typing import Dict, List, Mapping, Sequence
from airflow.decorators import task, dag
from ed_pipeline.modules import vitals

from pyspark.sql import SparkSession
from pyspark import sql 

# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively

@task()
def vitals_pull_task(
    spark: SparkSession,
    base_url: str,
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
    
    vitals_data, comp_data, = vitals.ed_vitals_pull(spark, base_url, vitals_codes, rand_sample_size, merge_with=merge_with)

    return {"main":vitals_data, "comp":comp_data}
