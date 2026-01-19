CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.recruitment_job
(
  recruitment_job_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each job.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, recruitment_job_num INT64 OPTIONS(description="A unique number from the source for each job.")
, job_title_name STRING OPTIONS(description="The title of the job.")
, job_grade_code STRING OPTIONS(description="A 3 digit code that identifies the grade of the job.")
, job_schedule_id INT64 OPTIONS(description="A unique identifier for the schedule of a job.")
, overtime_status_id INT64 OPTIONS(description="A unique identifier for each overtime status.")
, primary_facility_location_num NUMERIC OPTIONS(description="A unique identifier for the primary facility the job is associated with.")
, recruiter_user_sid INT64 OPTIONS(description="A unique identifier for each recruiter.")
, recruitment_job_parameter_sid INT64 OPTIONS(description="A unique identifier for each parameter of a job.")
, recruitment_position_sid INT64 OPTIONS(description="Unique identifier for each position.")
, fte_pct NUMERIC OPTIONS(description="This field maintains full time employee percent allocated for an HR Company. For example if FTE value > 0.7 it means employee is full time. If FTE value < 0.7 then employee is part time or temporary")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY recruitment_job_sid OPTIONS(description="Contains the details associated with a job in the recruiting system.");