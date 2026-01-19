CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_activity
(
  activity_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for an activity in EDW'),
  activity_object_status_id INT64 OPTIONS(description='Identifier for activity object status'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_activity_id INT64 NOT NULL OPTIONS(description='Unique identifier for an activity in Radiation Oncology'),
  activity_category_code_text STRING OPTIONS(description='Text for the activity category code'),
  activity_category_desc STRING OPTIONS(description='Description for the activity category code'),
  activity_code_text STRING OPTIONS(description='Text for activity code'),
  activity_desc STRING OPTIONS(description='Description for activity'),
  source_last_modified_date_time DATETIME OPTIONS(description='Date Time when data was last updated in source'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  effective_start_date_time DATETIME OPTIONS(description='Date time when record became active'),
  effective_end_date_time DATETIME OPTIONS(description='Date time when record became inactive'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_activity_id
OPTIONS(
  description='Contains information for radiation oncology activity'
);
