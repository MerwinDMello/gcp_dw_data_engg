CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_lookup_code
(
  master_lookup_sid INT64 NOT NULL OPTIONS(description='Master surrogate key generated for each lookup value.'),
  lookup_id INT64 NOT NULL OPTIONS(description='A unique identifier for each lookup column.'),
  lookup_code STRING NOT NULL OPTIONS(description='Unique code a particular column'),
  lookup_sub_code STRING OPTIONS(description='Unique sub code a particular column'),
  lookup_desc STRING OPTIONS(description='Description for the lookup column code'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY master_lookup_sid
OPTIONS(
  description='This table contains code and description of the columns for which lookup is done'
);
