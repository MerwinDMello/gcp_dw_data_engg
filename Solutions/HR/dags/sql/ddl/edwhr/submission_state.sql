CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.submission_state
(
  submission_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each submission."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically."),
  submission_state_id INT64 OPTIONS(description="A unique identifier for the status of the submission."),
  valid_to_date DATETIME NOT NULL OPTIONS(description="Date on which the record was invalidated."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_sid
OPTIONS(
  description="Contains the state history of each submission."
);