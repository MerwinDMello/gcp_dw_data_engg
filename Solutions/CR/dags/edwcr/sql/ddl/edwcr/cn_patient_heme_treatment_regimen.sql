CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_heme_treatment_regimen
(
  cn_patient_heme_diagnosis_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  regimen_id INT64 OPTIONS(description='Identifier for regimen prescribed to patient'),
  treatment_phase_id INT64 OPTIONS(description='Identifier for treatment phase of the patient'),
  pathway_var_reason_id INT64 OPTIONS(description='Identifier for variance reason if the patient is not compliant to a pathway'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  planned_start_date DATE OPTIONS(description='Planned start date for the treatment'),
  actual_start_date DATE OPTIONS(description='Actual start date for treatment'),
  drug_text STRING OPTIONS(description='Drugs included in the regimen'),
  cycle_num INT64 OPTIONS(description='Number of cycles in regimen'),
  cycle_length_num INT64 OPTIONS(description='Number for cycle length'),
  cycle_frequency_text STRING OPTIONS(description='Frequency of cycle length identified as in days or months'),
  ordinal_cycle_num INT64 OPTIONS(description='Indicates the ordinal position of the cycle record within the regimen'),
  pathway_ind STRING OPTIONS(description='Indication if the patient is on pathway'),
  pathway_text STRING OPTIONS(description='Text of the pathway the patient is on'),
  pathway_compliant_ind STRING OPTIONS(description='Indication if the patient is compliant with the pathway'),
  treatment_plan_document_date DATE OPTIONS(description='Date that the treatment plan was documented'),
  prior_plan_document_timeframe_ind STRING OPTIONS(description='Indication if the treatment plan was documented prior to or after treatment'),
  treatment_regimen_comment_text STRING OPTIONS(description='Treatment free text navigator comments'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains treatment regimen details for Hematology patient'
);
