
CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_submission_step (
step_id INT64 NOT NULL OPTIONS(description="A unique identifier for each step a submission can take.")
, submission_state_id INT64 NOT NULL OPTIONS(description="A unique identifier for the status of the submission ")
, active_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, step_code STRING OPTIONS(description="A code for a short description for each step an submission can take.")
, step_name STRING OPTIONS(description="The name for each step a submission can take.")
, step_short_name STRING OPTIONS(description="A short name for each step a submission can take.")
, step_desc STRING OPTIONS(description="A description for each step a submission can take.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Step_Id
OPTIONS(description="Contains a list of steps a submission can go through.");

