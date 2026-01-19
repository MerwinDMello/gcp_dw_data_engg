CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_diversity_inclusion (
leadership_level_id INT64 NOT NULL OPTIONS(description="Identifier for the Leadership Level.")
, match_level_num INT64 OPTIONS(description="Match level is defined based on number of departments that have same kind of code")
, match_level_desc STRING OPTIONS(description="This field maintains match level desc")
, lob_code STRING OPTIONS(description="The code corresponding to the LOB")
, job_class_code STRING NOT NULL OPTIONS(description="Unique user-defined job class Code. It identifies job class of a HR Company related Job Code.")
, job_code STRING OPTIONS(description="This is the job code for the job requisition.")
, leadership_level_desc STRING OPTIONS(description="Description of the Leadership Level")
, leadership_level_code STRING OPTIONS(description="The code corresponding to the Leadership Role")
, leadership_role_name STRING OPTIONS(description="The name of the Leadership Role")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Leadership_Level_Id
OPTIONS(description="This table maintains HR Insights for the tracking and reporting of diversity & inclusion metrics related to recruiting, retention, advancement, and inclusion.");
