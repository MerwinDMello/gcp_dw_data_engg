CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_navigator
(
  navigator_id INT64 NOT NULL OPTIONS(description='A unique identifier for each navigator.'),
  navigator_name STRING OPTIONS(description='The name of the navigator.'),
  navigator_3_4_id STRING OPTIONS(description='The 3-4 id of the navigator.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY navigator_id
OPTIONS(
  description='Contains the details around the navigator.'
);
