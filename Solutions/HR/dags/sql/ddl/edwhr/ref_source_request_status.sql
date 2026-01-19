CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_source_request_status
(
  source_request_status_id INT64 NOT NULL OPTIONS(description="Unique Identifier for each source request status."),
  source_request_status_desc STRING OPTIONS(description="This description shows the source request status llike posted , unposted , expired etc."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY source_request_status_id
OPTIONS(
  description="This table maintains the reference data for source request statuses like posted , unposted , expired etc."
);