CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.junc_submission_tracking_motive
(
  submission_tracking_sid INT64 NOT NULL OPTIONS(description="A unique identifier that tracks each step a submission takes."),
  tracking_motive_id INT64 NOT NULL OPTIONS(description="A unique identifier for each motive an event can have in the submission process."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded."),
  valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY submission_tracking_sid, tracking_motive_id, valid_from_date
OPTIONS(
  description="Contains the different motives each step of the submission process."
);