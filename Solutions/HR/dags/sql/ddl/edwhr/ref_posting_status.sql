CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_posting_status
(
posting_status_id INT64 NOT NULL OPTIONS(description="Unique Identifier for each posting status.")
, posting_status_code STRING OPTIONS(description="This code shows the posting status of job like posted , unposted , expired etc.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY posting_status_id
OPTIONS(description="This table maintains the reference data for Posting statuses like posted , unposted , expired etc.");