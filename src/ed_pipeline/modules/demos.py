import datetime
import math
from os import walk
from typing import Mapping, Sequence, Tuple

from pyspark import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff, lit, to_date, when
from ed_pipeline.utils import helpful_functions


def numerical_column_selection(
    df: sql.DataFrame, column: str, values: Sequence[int]
) -> sql.DataFrame:

    if len(values) == 2:
        return df.filter((col(column) >= lit(values[0])) & (col(column) <= lit(values[1])))
    else:
        return df.filter((col(column) == lit(values[0])))


def catagorical_column_selection(
    df: sql.DataFrame, column: str, values: Sequence[str]
) -> sql.DataFrame:
    return df.filter(col(column).isin(values))


def datetime_column_selection(
    df: sql.DataFrame, values: Sequence[datetime.datetime]
) -> sql.DataFrame:
    if len(values) == 2:
        return df.where(
            (df.visit_start_datetime >= lit(values[0]))
            & (df.visit_start_datetime <= lit(values[1]))
        )
    else:
        return df.where((df.visit_start_datetime == lit(values[0])))


def demographic_pull(
    spark: SparkSession,
    base_url: str,
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
) -> Tuple[sql.DataFrame, sql.DataFrame]:
    """Extracts demographics from OMOP dataset.

    Inputs a number of criteria for demographics columns (e.g. age, race, insurance) and outputs two pyspark DataFrames,
    one that contains the full patient dataset, and one that contains a subset based on `rand_sample_size`.
    Also requires the `base_url` where the OMOP data exists and a SparkSession.

    Args:
        spark: SparkSession
        base_url: base url where the OMOP data exists
        group_value: group by either visits (visit_occurrence_id) or person (person_id)
        age: filter by age range
        gender: filter by gender list
        race: filter by race list
        ethnicity: filter by ethnicity list
        insurance: filter by insurance list
        visit_date: filter by visit_date range
        visit_location: filter by visit location dict
        care_site: filter by care site dict
        rand_sample_size: size of random subset for distribution comparison

    Returns:
        A tuple of the full data and the randomly subsetted data (full, subset)
    """

    if group_value not in ["visit_occurrence_id", "person_id"]:
        raise BaseException("Invalid group_value. Options are visit_occurrence_id and person_id")

    visit_occurrence = helpful_functions.helpful_functions.merge_files(
        f"{base_url}/visit_occurrence", spark, show=False
    )
    visit_length_columns = [
        "person_id",
        "visit_occurrence_id",
        "visit_start_datetime",
        "visit_end_datetime",
    ]
    visit_length = visit_occurrence.select([col for col in visit_length_columns])
    visit_length = visit_length.withColumn(
        "visit_length",
        col("visit_end_datetime").cast("long") - col("visit_start_datetime").cast("long"),
    )

    print("Visit Data Loaded...")

    person = helpful_functions.merge_files(f"{base_url}/person", spark, show=False)
    demographics_cols = [
        "person_id",
        "birth_datetime",
        "death_datetime",
        "gender_source_value",
        "race_source_value",
        "ethnicity_source_value",
        "care_site_id",
        "location_id",
    ]
    demo = person.select([col for col in demographics_cols])

    print("Demographic Data Loaded...")

    payer_plan_period = helpful_functions.merge_files(
        f"{base_url}/payer_plan_period", spark, show=False
    )
    insurance_columns = [
        "person_id",
        "payer_plan_period_start_date",
        "payer_plan_period_end_date",
        "payer_source_value",
        "plan_source_value",
    ]
    insurance_df = payer_plan_period.select([col for col in insurance_columns])
    insurance_df = insurance_df.withColumn(
        "payer_plan_period_start_datetime", to_date("payer_plan_period_start_date")
    ).withColumn("payer_plan_period_end_datetime", to_date("payer_plan_period_end_date"))

    print("Insurance Data Loaded...")

    care_site_df = helpful_functions.merge_files(f"{base_url}/care_site", spark, show=False)
    care_site_columns = [
        "care_site_id",
        "care_site_name",
        "care_site_source_value",
        "place_of_service_source_value",
    ]
    care_site_df = care_site_df.select([col for col in care_site_columns])

    print("Care Site Data Loaded...")

    location = helpful_functions.merge_files(f"{base_url}/location", spark, show=False)
    location_columns = ["location_id", "address_1", "city", "state", "zip", "county", "country"]
    location = location.select([col for col in location_columns])

    print("Patient Locaiton Data Loaded...")

    if group_value == "visit_occurrence_id":
        insurance_df = insurance_df.join(
            visit_length, insurance_df.person_id == visit_length.person_id
        )
        insurance_df = insurance_df.where(
            (insurance_df.visit_start_datetime >= insurance_df.payer_plan_period_start_datetime)
            & (insurance_df.visit_end_datetime <= insurance_df.payer_plan_period_end_datetime)
        )
        insurance_df = insurance_df.distinct()
        insurance_columns = ["visit_occurrence_id", "payer_source_value", "plan_source_value"]
        insurance_df = insurance_df.select([col for col in insurance_columns])

        demo = demo.join(visit_length, on="person_id", how="outer")
        demo = demo.withColumn(
            "age",
            math.floor(datediff(col("visit_start_datetime"), col("birth_datetime")) / 365.25),
        )
        demo = demo.select(
            [
                col
                for col in [
                    "person_id",
                    "birth_datetime",
                    "death_datetime",
                    "gender_source_value",
                    "race_source_value",
                    "ethnicity_source_value",
                    "age",
                    "care_site_id",
                    "location_id",
                ]
            ]
        )
        demo = demo.distinct()

        print("Age and Insurance Provider Identified Data Loaded...")

    else:
        insurance_df = insurance_df.withColumn(
            "payer_plan_period_start_datetime", to_date("payer_plan_period_start_date")
        ).withColumn("payer_plan_period_end_datetime", to_date("payer_plan_period_end_date"))

        insurance_df = insurance_df.join(
            insurance_df.groupBy("person_id").agg(
                F.max("payer_plan_period_start_datetime").alias("payer_plan_period_start_datetime")
            ),
            on="payer_plan_period_start_datetime",
            how="leftsemi",
        )
        insurance_columns = ["person_id", "payer_source_value", "plan_source_value"]
        insurance_df = insurance_df.select([col for col in insurance_columns])

        now = datetime.datetime.now()
        demo = demo.withColumn(
            "age",
            when(
                col("death_datetime").isNull(),
                math.floor(datediff(lit(now), col("birth_datetime")) / 365.25),
            ).otherwise(
                math.floor(datediff(col("death_datetime"), col("birth_datetime")) / 365.25)
            ),
        )
        demo = demo.select(
            [
                col
                for col in [
                    "person_id",
                    "birth_datetime",
                    "death_datetime",
                    "gender_source_value",
                    "race_source_value",
                    "ethnicity_source_value",
                    "age",
                    "care_site_id",
                    "location_id",
                ]
            ]
        )
        demo = demo.distinct()

        print("Age and Insurance Provider Identified Data Loaded...")

    main_df = demo.join(visit_length, on="person_id", how="outer")
    print("Demographics and Visit Length Information Combined...")

    main_df = main_df.join(insurance_df, on=group_value, how="outer")
    print("Added Insurance Information Information...")

    main_df = main_df.join(care_site_df, on="care_site_id", how="outer")
    print("Added Care Site Information...")

    main_df = main_df.join(location, on="location_id", how="outer")
    print("Added Patient Locaiton Information...")

    if rand_sample_size == 0:
        patient_number = main_df.count()
        rand_df = main_df.limit(patient_number)
    else:
        rand_df = main_df.limit(rand_sample_size)

    if age != []:
        main_df = numerical_column_selection(main_df, "age", age)
        print("Filtered Data for Age...")

    if gender != []:
        main_df = catagorical_column_selection(main_df, "gender_source_value", gender)
        print("Filtered Data for Gender...")

    if race != []:
        main_df = catagorical_column_selection(main_df, "race_source_value", race)
        print("Filtered Data for Race...")

    if ethnicity != []:
        main_df = catagorical_column_selection(main_df, "ethnicity_source_value", ethnicity)
        print("Filtered Data for Ethnicity...")

    if insurance != []:
        main_df = catagorical_column_selection(main_df, "plan_source_value", insurance)
        print("Filtered Data for Insurance...")

    if visit_date != []:
        main_df = datetime_column_selection(main_df, visit_date)
        print("Filtered Data for Visit Date...")

    if visit_location != {}:
        for key in visit_location.keys():
            main_df = catagorical_column_selection(main_df, key, visit_location[key])
        print("Filtered Data for Care Site...")

    if care_site != {}:
        for key in care_site.keys():
            main_df = catagorical_column_selection(main_df, key, care_site[key])
        print("Filtered Data for Location...")

    return main_df, rand_df
