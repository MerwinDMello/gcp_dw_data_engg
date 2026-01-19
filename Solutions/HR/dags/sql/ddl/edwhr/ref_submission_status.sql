CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_submission_status
(
submission_status_id INT64 NOT NULL OPTIONS(description="A unique identifier for status of the submission process.")
, active_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, submission_state_id INT64 OPTIONS(description="A unique identifier for the status of the submission ")
, submission_status_code STRING OPTIONS(description="A code used for a short description for each status of the submission process.")
, submission_status_name STRING OPTIONS(description="A name for each status of the submission process.")
, submission_status_desc STRING OPTIONS(description="A description for each status of the submission process.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_status_id
OPTIONS(description="Contains a list of different statuses a submission can take.");