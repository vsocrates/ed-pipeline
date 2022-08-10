# import the logging module
import logging
from typing import List, Tuple

from ed_pipeline.utils.helpful_functions import merge_files
from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")


def ed_vitals_pull(
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
) -> Tuple[sql.DataFrame, sql.DataFrame]:
    """Extracts ED Vitals from OMOP dataset.

    Inputs a number of criteria for vitals and outputs two pyspark DataFrames,
    one that contains the full patient dataset, and one that contains a subset based on `rand_sample_size`.
    Also requires the `base_url` where the OMOP data exists and a SparkSession.

    Args:
        spark: SparkSession
        base_url: the base OMOP database URL
        vitals_codes: the list of vitals codes (default: Body weight, Diastolic blood pressure, Glasgow coma scale, Heart rate, Mean blood pressure, Systolic blood pressure, Q-T interval corrected, Body temperature, RR)
        rand_sample_size: size of random subset for distribution comparison

    Returns:
        A tuple of the full data and the randomly subsetted data (full, subset)
    """
    # TODO: Add time since start of visit selector

    measurement = merge_files(f"{base_url}/measurement", spark, show=False)
    concept = merge_files(f"{base_url}/concept", spark, show=False)
    vitals = measurement.join(
        concept, measurement.measurement_concept_id == concept.concept_id)
    task_logger.critical("Read in files...")

    vitals = vitals.filter(col("concept_id").isin(vitals_codes))
    vitals_columns = [
        "visit_occurrence_id",
        "measurement_source_value",
        "measurement_datetime",
        "value_as_number",
    ]
    vitals = vitals.select([col for col in vitals_columns])
    vitals2 = vitals.alias("vitals2")

    main_df = vitals.groupby("visit_occurrence_id", "measurement_source_value").agg(
        F.max("value_as_number").alias("Max_value"),
        F.min("value_as_number").alias("Min_value"),
        F.mean("value_as_number").alias("Mean_value"),
    )

    task_logger.critical("Grouped")
    first_pd = vitals2.groupby("visit_occurrence_id", "measurement_source_value").agg(
        F.min("measurement_datetime").alias("measurement_datetime")
    )
    last_pd = vitals2.groupby("visit_occurrence_id", "measurement_source_value").agg(
        F.max("measurement_datetime").alias("measurement_datetime")
    )
    first_final = (
        vitals2.join(
            first_pd,
            on=["visit_occurrence_id", "measurement_source_value",
                "measurement_datetime"],
            how="right",
        )
        .withColumnRenamed("value_as_number", "First_Value")
        .withColumnRenamed("measurement_datetime", "First_Datetime")
    )
    last_final = (
        vitals2.join(
            last_pd,
            on=["visit_occurrence_id", "measurement_source_value",
                "measurement_datetime"],
            how="right",
        )
        .withColumnRenamed("value_as_number", "Last_Value")
        .withColumnRenamed("measurement_datetime", "Last_Datetime")
    )
    task_logger.critical("First/Last finals")
    main_df = main_df.join(
        first_final, on=["visit_occurrence_id", "measurement_source_value"], how="outer"
    )
    main_df = main_df.join(
        last_final, on=["visit_occurrence_id", "measurement_source_value"], how="outer"
    )

    task_logger.critical("Join with main")
    if rand_sample_size == 0:
        patient_number = main_df.count()
        rand_df = main_df.limit(patient_number)
    else:
        rand_df = main_df.limit(rand_sample_size)
    task_logger.critical("Vital pull Done!")
    return main_df, rand_df
