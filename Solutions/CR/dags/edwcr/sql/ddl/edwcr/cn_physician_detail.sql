CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_physician_detail
(
  physician_id INT64 NOT NULL OPTIONS(description='A unique identifier for a physician.'),
  physician_name STRING OPTIONS(description='The physician name.'),
  physician_phone_num STRING OPTIONS(description='The physician phone number.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY physician_id
OPTIONS(
  description='The details for each physician.'
);
