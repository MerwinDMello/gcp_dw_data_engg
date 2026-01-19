CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.junc_recruitment_job_board
(
  recruitment_job_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each job."),
  job_board_id INT64 NOT NULL OPTIONS(description="Unique Job Board Id"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically."),
  posting_board_type_id INT64 OPTIONS(description="Unique Identifier for each board type."),
  posting_status_id INT64 OPTIONS(description="Unique Identifier for each posting status."),
  valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated."),
  posting_date DATE OPTIONS(description="Job posting date."),
  unposting_date DATE OPTIONS(description="Job unposting date."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY recruitment_job_sid, job_board_id, valid_from_date
OPTIONS(
  description="This table contains the posting and board details associated with a job in the recruiting system."
);