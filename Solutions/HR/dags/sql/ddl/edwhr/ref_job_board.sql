CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_job_board
(
job_board_id INT64 NOT NULL OPTIONS(description="Unique job board identifier")
, job_board_type_id INT64 OPTIONS(description="Contains job board type identifier related to job board.")
, recruitment_source_id INT64 OPTIONS(description="Contains recruitment source identifier related to job board.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY job_board_id
OPTIONS(description="Unique list of job board id and related job recuritment info is maintained in this table.");