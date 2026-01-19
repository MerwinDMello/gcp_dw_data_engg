CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_family_history
(
  cn_patient_family_history_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  family_history_query_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient history category.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  family_history_value_text STRING OPTIONS(description='The value for each query or measure of patient history categories.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, coid, company_code
OPTIONS(
  description='Contains oncology details associated with a patients family history.'
);
