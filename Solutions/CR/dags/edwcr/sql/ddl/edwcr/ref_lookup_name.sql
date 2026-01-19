CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_lookup_name
(
  lookup_sid INT64 NOT NULL OPTIONS(description='A unique surrogate key for each lookup column'),
  lookup_name STRING OPTIONS(description='Name of the column for which lookup is done'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
OPTIONS(
  description='This table contains name of the column for which lookup is done'
);
