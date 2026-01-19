CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.submission_tracking_status
(
submission_tracking_sid INT64 NOT NULL OPTIONS(description=" A unique identifier that tracks each step a submission takes.")
, valid_from_date DATETIME NOT NULL OPTIONS(description=" Date the record is valid from based on when it was loaded.")
, valid_to_date DATETIME OPTIONS(description=" Date the record is valid to based on when it was loaded.")
, submission_status_id INT64 OPTIONS(description=" A unique identifier for status of the submission process.")
, source_system_code STRING NOT NULL OPTIONS(description=" A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_tracking_sid
OPTIONS(description=" Table contains the status of each step in the submission process.");