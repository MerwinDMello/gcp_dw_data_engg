CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_complication
(
  cn_patient_complication_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  therapy_type_id INT64 OPTIONS(description='A unique identifier for each therapy type.'),
  outcome_result_id INT64 OPTIONS(description='A unique identifier for each outcome of the complication.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  complication_date DATE OPTIONS(description='The date the complication took place.'),
  treatment_stopped_ind STRING OPTIONS(description='Indicates if the treatment was stopped due to a complication.'),
  complication_text STRING OPTIONS(description='A free text field identifing the complication.'),
  specific_complication_text STRING OPTIONS(description='A free text field that identifies the specific complication.'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY core_record_type_id, nav_patient_id, tumor_type_id, diagnosis_result_id
OPTIONS(
  description='Contains the details associated with any complications a patient had.'
);
