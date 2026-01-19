CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_physician_role
(
  physician_id INT64 NOT NULL OPTIONS(description='A unique identifier for a physician.'),
  physician_role_code STRING NOT NULL OPTIONS(description='The role of the physician. Attending, Admitting, etc...'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY physician_id, physician_role_code
OPTIONS(
  description='This table contains all the roles a physician could play.'
);
