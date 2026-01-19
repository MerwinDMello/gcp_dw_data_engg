CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_job_schedule
(
job_schedule_id INT64 NOT NULL OPTIONS(description="A unique identifier for the schedule of a job.")
, active_sw INT64 OPTIONS(description="Numeric boolean to where 0 = false/no and 1 = true/yes")
, job_schedule_code STRING OPTIONS(description="A code that represents the schedule of a job.")
, job_schedule_desc STRING OPTIONS(description="The description for the schedule of a job.")
, job_schedule_seq_num INT64 OPTIONS(description="A sequence number from the source for each job schedule.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Job_Schedule_Id
OPTIONS(description="Contains a distinct list of job schedules.");