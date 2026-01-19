CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_job_template_status
(
job_template_status_id INT64 NOT NULL OPTIONS(description="A unique identifier for each job template status.")
, job_template_status_desc STRING OPTIONS(description="The description of th e job template status.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY job_template_status_id
OPTIONS(description="Contains the distinct list of statuses for a job template.");