
# Dictionary of all dataframes and the columns they contain
df_to_columns = {
    'care_site': ['care_site_id', 'care_site_name', 'place_of_service_concept_id', 'location_id', 'care_site_source_value', 'place_of_service_source_value'], 
    
    'concept': ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id', 'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason'], 
    
    'concept_ancestor': ['ancestor_concept_id', 'descendant_concept_id', 'min_levels_of_separation', 'max_levels_of_separation'], 
    
    'concept_class': ['concept_class_id', 'concept_class_name', 'concept_class_concept_id'], 
    
    'concept_relationship': ['concept_id_1', 'concept_id_2', 'relationship_id', 'valid_start_date', 'valid_end_date', 'invalid_reason'],
    
    'concept_synonym': ['concept_id', 'concept_synonym_name', 'language_concept_id'], 
    
    'condition_occurrence': ['person_id', 'condition_occurrence_id', 'condition_occurrence_source_id', 'condition_concept_id', 'condition_start_date', 'condition_start_datetime', 'condition_end_date', 'condition_end_datetime', 'condition_type_concept_id', 'condition_status_concept_id', 'stop_reason', 'provider_id', 'visit_occurrence_id', 'visit_detail_id', 'condition_source_value', 'condition_source_concept_id', 'condition_status_source_value'],
    
    'csn_map': ['visit_occurrence_id', 'csn'], 
    
    'domain': ['domain_id', 'domain_name', 'domain_concept_id'], 
    
    'drug_exposure': ['person_id', 'drug_exposure_id', 'drug_concept_id', 'drug_exposure_start_date', 'drug_exposure_start_datetime', 'drug_exposure_end_date', 'drug_exposure_end_datetime', 'verbatim_end_date', 'drug_type_concept_id', 'stop_reason', 'refills', 'quantity', 'days_supply', 'sig', 'route_concept_id', 'lot_number', 'provider_id', 'visit_occurrence_id', 'visit_detail_id', 'drug_source_value_code', 'drug_source_value_name', 'drug_generic_source_value_code', 'drug_generic_source_value_name', 'drug_source_concept_id', 'route_source_value', 'dose_unit_source_value', 'drug_class_source_value'], 
    
    'drug_strength': ['drug_concept_id', 'ingredient_concept_id', 'amount_value', 'amount_unit_concept_id', 'numerator_value', 'numerator_unit_concept_id', 'denominator_value', 'denominator_unit_concept_id', 'box_size', 'valid_start_date', 'valid_end_date', 'invalid_reason'],
    
    'location': ['location_id', 'address_1', 'address_2', 'city', 'state', 'zip', 'county', 'country', 'location_source_value', 'latitude', 'longitude'],
    
    'measurement': ['person_id', 'measurement_id', 'specimen_id', 'measurement_concept_id', 'measurement_date', 'measurement_datetime', 'order_datetime', 'measurement_time', 'measurement_type_concept_id', 'operator_concept_id', 'value_as_number', 'value_as_string', 'value_as_concept_id', 'unit_concept_id', 'range_low', 'range_high', 'provider_id', 'visit_occurrence_id', 'visit_detail_id', 'measurement_source_value', 'measurement_source_value_alt', 'measurement_source_concept_id', 'unit_source_value', 'value_source_value'], 
    
    'mrn_map': ['person_id', 'mrn'],
    
    'note': ['person_id', 'note_id', 'note_event_id', 'note_event_field_concept_id', 'note_date', 'note_datetime', 'note_type_concept_id', 'note_class_concept_id', 'note_title', 'note_text', 'encoding_concept_id', 'language_concept_id', 'provider_id', 'visit_occurrence_id', 'visit_detail_id', 'note_source_value'], 
    
    'observation': ['person_id', 'observation_id', 'specimen_id', 'observation_concept_id', 'observation_date', 'observation_datetime', 'observation_type_concept_id', 'value_as_number', 'value_as_string', 'value_as_concept_id', 'qualifier_concept_id', 'unit_concept_id', 'provider_id', 'visit_occurrence_id', 'visit_detail_id', 'observation_source_value', 'observation_source_concept_id', 'unit_source_value', 'qualifier_source_value', 'observation_event_id', 'obs_event_field_concept_id', 'value_as_datetime', 'value_source_value'],
    
    'payer_plan_period': ['person_id', 'payer_plan_period_id', 'contract_person_id', 'payer_plan_period_start_date', 'payer_plan_period_end_date', 'payer_concept_id', 'plan_concept_id', 'contract_concept_id', 'sponsor_concept_id', 'stop_reason_concept_id', 'payer_source_value', 'payer_source_concept_id', 'plan_source_value', 'plan_source_concept_id', 'contract_source_value', 'contract_source_concept_id', 'sponsor_source_value', 'sponsor_source_concept_id', 'family_source_value', 'stop_reason_source_value', 'stop_reason_source_concept_id', 'phenotype_start_date', 'phenotype_end_date'], 
    
    'person': ['person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth', 'day_of_birth', 'date_of_birth', 'birth_datetime', 'death_datetime', 'race_concept_id', 'ethnicity_concept_id', 'location_id', 'provider_id', 'care_site_id', 'person_source_value', 'gender_source_value', 'gender_source_concept_id', 'race_source_value', 'race_source_concept_id', 'ethnicity_source_value', 'ethnicity_source_concept_id'],
    
    'phenotype': ['phenotype_uri', 'person_id', 'mrn', 'visit_occurrence_id', 'csn', 'order_id', 'specimen_id', 'birth_datetime', 'phenotype_start_date', 'phenotype_end_date'], 
    
    'procedure_occurrence': ['person_id', 'procedure_occurrence_id', 'procedure_concept_id', 'procedure_source_id', 'procedure_date', 'procedure_datetime', 'procedure_type_concept_id', 'modifier_concept_id', 'quantity', 'provider_id', 'visit_occurrence_id', 'visit_detail_id', 'procedure_source_value', 'procedure_source_concept_id', 'modifier_source_value', 'status_source_value'], 
    
    'relationship': ['relationship_id', 'relationship_name', 'is_hierarchical', 'defines_ancestry', 'reverse_relationship_id', 'relationship_concept_id'],
    
    'specimen': ['person_id', 'specimen_id', 'specimen_concept_id', 'specimen_type_concept_id', 'specimen_date', 'specimen_datetime', 'quantity', 'unit_concept_id', 'anatomic_site_concept_id', 'disease_status_concept_id', 'collection_container_concept_id', 'specimen_source_id', 'specimen_source_value', 'unit_source_value', 'anatomic_site_source_value', 'disease_status_source_value', 'collection_container_source_value'], 
    
    'visit_occurrence': ['person_id', 'visit_occurrence_id', 'visit_concept_id', 'visit_start_date', 'visit_start_datetime', 'visit_end_date', 'visit_end_datetime', 'visit_type_concept_id', 'provider_id', 'care_site_id', 'visit_source_value', 'visit_source_concept_id', 'admitted_from_concept_id', 'admitted_from_source_value', 'discharge_to_source_value', 'discharge_to_concept_id', 'preceding_visit_occurrence_id'], 
    
    'vocabulary': ['vocabulary_id', 'vocabulary_name', 'vocabulary_reference', 'vocabulary_version', 'vocabulary_concept_id']}

# List of possible dataframes.
df_names = ['care_site', 'concept', 'concept_ancestor', 'concept_class', 'oncept_relationship', 'concept_synonym', 'condition_occurrence', 'csn_map', 'domain', 'drug_exposure', 'drug_strength', 'location', 'measurement', 'mrn_map', 'note', 'observation', 'payer_plan_period', 'person', 'phenotype', 'procedure_occurrence', 'relationship', 'specimen', 'visit_occurrence', 'vocabulary']