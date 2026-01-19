CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.sourcing_request (
sourcing_request_sid INT64 NOT NULL OPTIONS(description="Unique sourcing request system generated identifier. Its a combination of requisition number and job board id ")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, recruitment_requisition_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each requisition coming from the recruitment system.")
, job_board_id INT64 NOT NULL OPTIONS(description="Unique job board number")
, source_request_status_id INT64 OPTIONS(description="Unique Identifier for each posting status.")
, job_board_type_id INT64 OPTIONS(description="Unique Identifier for each board type.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, posting_date DATE OPTIONS(description="Job posting date.")
, unposting_date DATE OPTIONS(description="Job unposting date.")
, creation_date DATE OPTIONS(description="Record creation date")
, requisition_num INT64 NOT NULL OPTIONS(description="A unique number  for each job.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY sourcing_request_sid, valid_from_date
OPTIONS(description="This table contains the sourcing request details associated with a job in the recruiting system.");
