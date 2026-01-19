CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_patient_care_position (
job_code STRING NOT NULL OPTIONS(description="It is the unique Job code maintained in Lawson")
, job_title_text STRING NOT NULL OPTIONS(description="This field maintains job title ")
, job_code_desc STRING OPTIONS(description="This field maintains job code description ")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Job_Code, Job_Title_Text
OPTIONS(description="This table maintains Patient care job role codes and description provided by HR resource.");
