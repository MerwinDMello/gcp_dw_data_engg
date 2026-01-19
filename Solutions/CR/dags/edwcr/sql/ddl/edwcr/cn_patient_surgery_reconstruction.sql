CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_surgery_reconstruction
(
  cn_patient_surgery_recstr_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  surgery_recstr_side_id INT64 OPTIONS(description='Identifies which breast the reconstructive surgery occured.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  recstr_date DATE OPTIONS(description='Date of reconstruction.'),
  surgery_recstr_type_text STRING OPTIONS(description='The different types of reconstructive surgery.'),
  declined_ind STRING OPTIONS(description='If the p'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id
OPTIONS(
  description='Contains the details behind any breast reconstructive surgery.'
);
