CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_survey_mode (
survey_mode_code STRING NOT NULL OPTIONS(description="It maintains unique list of survey mode code. ")
, survey_mode_desc STRING NOT NULL OPTIONS(description="Description of Survey Mode")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Survey_Mode_Code
OPTIONS(description="This table maintains unique list of survey type code.");
