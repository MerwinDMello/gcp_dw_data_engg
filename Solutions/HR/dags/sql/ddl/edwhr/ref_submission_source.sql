CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_submission_source
(
  submission_source_code STRING NOT NULL OPTIONS(description="Code from the source that shows where an applicant found a requisition."),
  submission_source_desc STRING OPTIONS(description="Description of where the applicant found the requisition."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY submission_source_code
OPTIONS(
  description="Reference table that lists all possible sources where an applicant can find a requisition."
);