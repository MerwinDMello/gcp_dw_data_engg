CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_patient_history
(
  patient_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key for each patient'),
  history_query_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient history category.'),
  history_value_text STRING OPTIONS(description='The value for each query or measure of patient history categories.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_sk, history_query_id
OPTIONS(
  description='Contains history details of the patient'
);
