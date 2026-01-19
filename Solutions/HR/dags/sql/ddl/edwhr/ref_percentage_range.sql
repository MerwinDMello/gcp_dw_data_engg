CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_percentage_range
(
  percentage_range_sid INT64 NOT NULL OPTIONS(description="This is the ETL generated sequence number for unique list of Travel percentage range maintained."),
  percentage_range_desc STRING OPTIONS(description="This is the description of Travel Percentage Range"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY percentage_range_sid
OPTIONS(description="This table maintains unique percentage range.");