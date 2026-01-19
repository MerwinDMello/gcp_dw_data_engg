CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.submission_tracking
(
submission_tracking_sid INT64 NOT NULL OPTIONS(description="A unique identifier that tracks each step a submission takes.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, candidate_profile_sid INT64 OPTIONS(description="A unique identifier for each candidate profile.")
, submission_tracking_num INT64 OPTIONS(description="A unique number from the source that tracks each step a submission takes.")
, creation_date_time DATETIME OPTIONS(description="Date and time of when the submission step was first created.")
, event_date_time DATETIME OPTIONS(description="Date and time of the step the submission took in the workflow process.")
, event_detail_text STRING OPTIONS(description="Comments associated with the step the submission took in the workflow process.")
, submission_event_id INT64 OPTIONS(description="A unique identifier for each event that occurred to the submission.")
, tracking_user_sid INT64 OPTIONS(description="A unique identifier for each user.")
, tracking_step_id INT64 OPTIONS(description="A unique identifier for each step an submission can take in the application process.")
, tracking_workflow_id INT64 OPTIONS(description="A unique identifier for the workflow of current submission.")
, step_reverted_ind STRING OPTIONS(description="Designates whether a step in the application process has been reverted. Y = reverted, N = not reverted.")
, sub_status_desc STRING OPTIONS(description="Contains the sub status description. ")
, moved_by_text STRING OPTIONS(description="Contains the identifier of the person or system that made the change. ")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_tracking_sid
OPTIONS(description="Table provides all the activity/history on the Submission Profile.");