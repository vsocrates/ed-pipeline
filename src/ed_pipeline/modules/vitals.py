def ed_vitals_pull(spark, rand_sample_size=0, merge_with=None):

    codes = [3025315, 3012888, 3032652, 3027018, 3027598, 3024171, 3004249, 3026258, 3020891]
    # Body weight, Diastolic blood pressure, Glasgow coma scale, Heart rate, Mean blood pressure, Systolic blood pressure, Q-T interval corrected, Body temperature, RR

    # First and Last vitals for the visit and keep time for each measurement

    vitals = merge_files("/home/jupyter/omop-ed-datapipeline/measurement", spark, show=False)
    concept = merge_files("/home/jupyter/omop-ed-datapipeline/concept", spark, show=False)
    vitals = vitals.join(concept, vitals.measurement_concept_id == concept.concept_id)
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
