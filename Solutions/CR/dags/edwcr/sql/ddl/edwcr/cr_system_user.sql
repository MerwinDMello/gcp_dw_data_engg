CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_system_user
(
  security_id INT64 NOT NULL OPTIONS(description='Unique identifier which identifies the system user'),
  user_id_code STRING OPTIONS(description='A unique identifier code for each user.'),
  user_first_name STRING OPTIONS(description='First name of the system user'),
  user_last_name STRING OPTIONS(description='Last name of the system user'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY security_id
OPTIONS(
  description='Contains details of the system user'
);
