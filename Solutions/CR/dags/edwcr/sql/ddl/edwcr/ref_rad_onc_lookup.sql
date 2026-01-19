CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_lookup
(
  lookup_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a radiation oncology lookup data in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_lookup_id INT64 NOT NULL OPTIONS(description='Unique identifier for a lookup in Radiation Oncology source'),
  table_name STRING OPTIONS(description='Name of the table for which lookup is stored'),
  lookup_code_text STRING OPTIONS(description='Text for the lookup code'),
  lookup_desc STRING OPTIONS(description='Description for the lookup'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_lookup_id
OPTIONS(
  description='Contains information for radiation oncology lookup data'
);
