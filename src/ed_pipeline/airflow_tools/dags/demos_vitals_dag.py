import sys
sys.path.append("/Users/vsocrates/Documents/Yale/EDPipeline/ed-pipeline/src")

import json
from typing import Mapping, Sequence

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.decorators import task

import datetime
from textwrap import dedent

from airflow.utils import dates
from ed_pipeline.modules import vitals

from ed_pipeline.airflow_tools.tasks import demos_tasks
from ed_pipeline.airflow_tools.tasks import common
from  ed_pipeline.airflow_tools.tasks import vitals_tasks

"""Definition of DAG to Extract Demos and Vitals.

We will be using the Airflow 2.0 new TaskFlow API because it makes it significantly easier to pass data between tasks (Xcoms)
and define various parts of the dag. It also makes more sense to look at due to abstractions and method defintions.
"""
@dag(
    schedule_interval=None,
    start_date=dates.days_ago(2),
    catchup=False,
    tags=['ed_demo_vitals'],
)
def demos_vitals_pull_dag(base_url: str, 
    group_value: str,
    age: Sequence[int] = [],
    gender: Sequence[str] = [],
    race: Sequence[str] = [],
    ethnicity: Sequence[str] = [],
    insurance: Sequence[str] = [],
    visit_date: Sequence[datetime.datetime] = [],
    visit_location: Mapping[str, str] = {},
    care_site: Mapping[str, str] = {},
    rand_sample_size: int = 0):
    """
    ### ED-Pipeline Demos + Vitals Pull
    This is a Airflow DAG that extracts demographics and vitals from the OMOP dataset, along with some 
    TODO: quality checks, and initialization of a Spark session (necessary for data pull). 

    It has the following required inputs:  

        base_url: base url of the OMOP dataset
        group_value: group by either visits (visit_occurrence_id) or person (person_id)

    It also have the following optional "filtering" parameters: 

        age: filter by age range
        gender: filter by gender list
        race: filter by race list
        ethnicity: filter by ethnicity list
        insurance: filter by insurance list
        visit_date: filter by visit_date range
        visit_location: filter by visit location dict
        care_site: filter by care site dict
        rand_sample_size: size of random subset for distribution comparison

    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    spark = common.start_spark_session("test")
    demos_paired_df = demos_tasks.demos_pull_task(spark, base_url, group_value,  age, gender,  race,  ethnicity,  insurance,  
        visit_date,  visit_location,  care_site,  rand_sample_size)

    vitals_paired_df = vitals_tasks.vitals_pull_task(spark, base_url)

    main_merged = common.merge_data_pull.override(task_id=f"main_data_merge")([demos_paired_df['main'], vitals_paired_df['main']],
        "visit_occurrence_id", merge_method="left")

    comp_merged = common.merge_data_pull.override(task_id=f"compare_data_merge")([demos_paired_df['comp'], vitals_paired_df['comp']],
        "visit_occurrence_id", merge_method="left")

    print(main_merged['return_value'])
    print(comp_merged['return_value'])


demos_vitals_pull = demos_vitals_pull_dag("/home/jupyter/omop-ed-datapipeline", "visit_occurrence_id")

