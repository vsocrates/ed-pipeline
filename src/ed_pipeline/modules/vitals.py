from typing import List

from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from src.ed_pipeline.utils.helpful_functions import merge_files


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
    merge_with: sql.DataFrame = None,
) -> Tuple[sql.DataFrame, sql.DataFrame]:
    """Extracts vitals from OMOP dataset.

    Extracts data with list of `vitals_codes` relevant to vitals and outputs two pyspark DataFrames,
    one that contains the full patient dataset, and one that contains a subset based on `rand_sample_size`.
    Also requires the `base_url` where the OMOP data exists and a SparkSession.

    Args:
        spark: SparkSession
        base_url: base URL where the OMOP data is located
        vitals_codes: list of vitals codes to extract (default Body weight, Diastolic blood pressure, Glasgow coma scale, Heart rate, Mean blood pressure, Systolic blood pressure, Q-T interval corrected, Body temperature, RR)
        rand_sample_size: size of random subset of data
        merge_with: another pyspark DataFrame to merge with, if wanted

    Returns:
        A tuple of the full data and the randomly subsetted data (full, subset)

    """
    # First and Last vitals for the visit and keep time for each measurement

    vitals = merge_files(f"{base_url}/measurement", spark, show=False)
    measurement = vitals.alias("measurement")
    concept = merge_files(f"{base_url}/concept", spark, show=False)
    vitals = vitals.join(concept, vitals.measurement_concept_id == concept.concept_id)

    # TODO: Figure out why this was done like this, fixed above with alias
    vitals = measurement.filter(col("concept_id").isin(codes))
    main_df = vitals.groupby("visit_occurrence_id", "measurement_source_value").agg(
        F.max("value_as_number").alias("Max_value"),
        F.min("value_as_number").alias("Min_value"),
        F.mean("value_as_number").alias("Mean_value"),
    )

    if rand_sample_size == 0:
        patient_number = main_df.count()
        rand_df = main_df.limit(patient_number)
    else:
        rand_df = main_df.limit(rand_sample_size)

    if merge_with != None:
        main_df = merge_with.join(main_df, on="visit_occurrence_id", how="left")

    return main_df, rand_df
