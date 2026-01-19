CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_national_service_center
(
  nsc_id INT64 NOT NULL OPTIONS(description='A unique identifier for National Service Center'),
  nsc_code STRING NOT NULL OPTIONS(description='National Service Center Codes'),
  nsc_sub_code STRING OPTIONS(description='National Service Center subcode'),
  nsc_desc STRING OPTIONS(description='Description of National Service Center'),
  nsc_category_text STRING OPTIONS(description='Category of National Service Center'),
  nsc_sub_category_text STRING OPTIONS(description='Sub category of National Service Center'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nsc_id
OPTIONS(
  description='This table contains National Service Center Codes and descriptions'
);
