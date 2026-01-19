CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_navque_reason
(
  navque_reason_id INT64 NOT NULL OPTIONS(description='Unique identifier for NavQue reason patient was not a candidate for Navigation'),
  navque_reason_name STRING NOT NULL OPTIONS(description='Name of NavQue reason patient was not a candidate for Navigation'),
  navque_reason_desc STRING OPTIONS(description='Description of NavQue reason patient was not a candidate for Navigation'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY navque_reason_id
OPTIONS(
  description='This table contains reason and descriptions performed by Navigator'
);
