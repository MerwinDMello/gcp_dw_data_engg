CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_heme_disease_assess
(
  cn_patient_heme_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  test_type_id INT64 OPTIONS(description='Identifier for disease assessment test type'),
  sample_type_id INT64 OPTIONS(description='Identifier for sample source type'),
  disease_assess_source_id INT64 OPTIONS(description='Identifier for source of the Disease Assessment'),
  disease_assess_facility_id INT64 OPTIONS(description='The Facility where the test was performed'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  collection_date DATE OPTIONS(description='The date of disease assessment collection'),
  disease_status_id INT64 OPTIONS(description='Status of disease at time of assessment'),
  treatment_status_id INT64 OPTIONS(description='Treatment status at the time of assessment\r'),
  initial_diagnosis_ind STRING OPTIONS(description='Indicates if disease assessment is related to initial diagnosis'),
  disease_monitoring_ind STRING OPTIONS(description='Indicates if test is related to disease monitoring '),
  comment_text STRING OPTIONS(description='Free Text comments about disease assessments'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains disease asessment data for Hematology patient'
);
