CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_navque_action
(
  navque_action_id INT64 NOT NULL OPTIONS(description='Unique identifier for navigator action'),
  navque_action_name STRING NOT NULL OPTIONS(description='Action Navigator took in Navigation queue'),
  navque_action_desc STRING OPTIONS(description='Description of action Navigator took within NavQue'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY navque_action_id
OPTIONS(
  description='This table contains action and descriptions performed by Navigator'
);
