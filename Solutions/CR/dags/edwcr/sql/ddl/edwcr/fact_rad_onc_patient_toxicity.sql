CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient_toxicity
(
  fact_patient_toxicity_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for fact data for patient toxicity in EDW'),
  toxicity_component_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for toxicity component in EDW'),
  toxicity_assessment_type_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for toxicity assessment type in EDW'),
  activity_transaction_sk INT64 OPTIONS(description='Unique surrogate key generated for an activity transaction in EDW'),
  patient_sk INT64 OPTIONS(description='Unique surrogate key generated for each patient in EDW'),
  scheme_id INT64 OPTIONS(description='Identifier for scheme'),
  toxicity_cause_certainty_type_id INT64 OPTIONS(description='Identifier for toxicity cause certainty type'),
  toxicity_cause_type_id INT64 OPTIONS(description='identifier for toxicity cause type'),
  diagnosis_code_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a diagnosis code in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_fact_patient_toxicity_id INT64 NOT NULL OPTIONS(description='Unique identifier for patient toxicity data in Radiation Oncology in source'),
  assessment_date_time DATETIME OPTIONS(description='Date time for assessment'),
  toxicity_effective_date DATE OPTIONS(description='Effective date of toxicity'),
  toxicity_grade_num INT64 OPTIONS(description='Grade number for toxicity'),
  valid_entry_ind STRING OPTIONS(description='Indicator for valid entry'),
  toxicity_approved_date_time DATETIME OPTIONS(description='Date time for approved toxicity'),
  assessment_performed_date_time DATETIME OPTIONS(description='Date time for assessment performed'),
  toxicity_reason_text STRING OPTIONS(description='Text for toxicity reason'),
  toxicity_approved_ind STRING OPTIONS(description='Indicator for approved toxicity'),
  toxicity_header_valid_entry_ind STRING OPTIONS(description='Indicator for valid entry for toxicity'),
  revision_num INT64 OPTIONS(description='Number for revision'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_fact_patient_toxicity_id
OPTIONS(
  description='Contains fact information of Radiation Oncology for patient toxicity'
);
