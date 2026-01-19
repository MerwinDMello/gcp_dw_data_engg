CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_timeframe
(
  timeframe_id INT64 NOT NULL OPTIONS(description="A unqiue id for each timeframe reference."),
  timeframe_desc STRING OPTIONS(description="The description of hte timeframe."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY timeframe_id
OPTIONS(description="Contains different timeframes.");