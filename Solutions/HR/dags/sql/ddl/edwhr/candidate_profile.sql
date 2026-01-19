CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_profile
(
candidate_profile_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each candidate profile.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, candidate_sid INT64 OPTIONS(description="Unique identifier for each candidate.")
, profile_medium_id INT64 OPTIONS(description="A unique identifier for the medium in which a profile was submitted to HCA.")
, candidate_profile_num NUMERIC OPTIONS(description="A unique identifier for each candidate profile from the source.")
, submission_date DATE OPTIONS(description="The date of the submission tied to the application profile.")
, submission_date_time DATETIME OPTIONS(description="The date and time of the submission tied to the application profile.")
, completion_date DATE OPTIONS(description="Date the submission profile was completed by the candidate.")
, completion_date_time DATETIME OPTIONS(description="Date and time the submission profile was completed by the candidate.")
, creation_date DATE OPTIONS(description="Date the submission profile was created by the candidate.")
, creation_date_time DATETIME OPTIONS(description="Date and time the submission profile was created by the candidate.")
, recruitment_source_id INT64 OPTIONS(description="A unique identifier of the recruiting source. Ex. Internet, job board, etc.")
, recruitment_source_auto_filled_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, valid_to_date DATETIME NOT NULL OPTIONS(description="Date on which the record was invalidated.")
, requisition_num INT64 OPTIONS(description="Unique id of an lawson generated requisition.")
, job_application_num INT64 OPTIONS(description="Number corresponding with the job application filled out by the candidate. This number only changes when the original application is deleted or withdrawn.")
, candidate_num INT64 OPTIONS(description="The unique number for a candidate coming from the source.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY candidate_profile_sid
OPTIONS(description="Table contains the profile of a candidate tied to a specific submission. ");