CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_nursing_director (
job_code STRING NOT NULL OPTIONS(description="This field captures job code")
, director_grouping_desc STRING OPTIONS(description="This field captures director grouping description")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Job_Code
OPTIONS(description="This table maintains nursing directors job codes and the director grouping.");
