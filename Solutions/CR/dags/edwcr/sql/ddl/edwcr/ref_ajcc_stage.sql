CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage
(
  ajcc_stage_id INT64 NOT NULL OPTIONS(description='A unique identifier for American Joint Committee On Cancer Stage'),
  ajcc_stage_code STRING OPTIONS(description='American Joint Committee On Cancer Stage Codes'),
  ajcc_stage_sub_code STRING OPTIONS(description='American Joint Committee On Cancer stage subcode'),
  ajcc_stage_desc STRING OPTIONS(description='Description for American Joint Committee On Cancer Stage'),
  ajcc_stage_group_id INT64 OPTIONS(description='Group Identifier for American Joint Committee On Cancer Stage'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY ajcc_stage_id
OPTIONS(
  description='This table contains American Joint Committee On Cancer Stage codes and descriptions'
);
