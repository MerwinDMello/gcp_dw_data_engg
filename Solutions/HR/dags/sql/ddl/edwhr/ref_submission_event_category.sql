CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_submission_event_category
(
  submission_event_category_id INT64 NOT NULL OPTIONS(description="A unique identifier for each event category that occurred to the submission"),
  submission_event_category_code STRING OPTIONS(description="A code used for a short description for an event category."),
  submission_event_category_desc STRING OPTIONS(description="The description for an event category."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_event_category_id
OPTIONS(
  description="Contains a list of event types each submission can go through."
);