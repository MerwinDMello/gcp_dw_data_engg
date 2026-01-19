CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_diagnosis
(
  cn_patient_diagnosis_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  diagnosis_detail_id INT64 OPTIONS(description='Unique identifier for diagnosis detail'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  general_diagnosis_name STRING OPTIONS(description='A free text field to provide added detail to the patient diagnosis.'),
  diagnosis_date DATE OPTIONS(description='Date on which diagnosis was done'),
  diagnosis_side_id INT64 OPTIONS(description='Identifier indicating side on which diagnosis is performed'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains free text fields related to the diagnosis. Sourced from the patient daignosis table on SQL Server.'
);
