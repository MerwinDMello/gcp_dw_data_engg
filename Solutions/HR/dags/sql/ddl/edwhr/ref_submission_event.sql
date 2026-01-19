CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_submission_event
(
  submission_event_id INT64 NOT NULL OPTIONS(description="A unique identifier for each event that occurred to the submission."),
  submission_event_category_id INT64 OPTIONS(description="A unique identifier for each event category that occurred to the submission."),
  submission_event_code STRING OPTIONS(description="A code used for short description that represent the event that occurred to the submission."),
  submission_event_desc STRING OPTIONS(description="The description of the event that occurred to the submission."),
  submission_event_detail_desc STRING OPTIONS(description="The detail description of the event that occurred to the submission."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_event_id
OPTIONS(
  description="Contains a list of events that each submission goes through."
);