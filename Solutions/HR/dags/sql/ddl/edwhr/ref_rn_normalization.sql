CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_rn_normalization (
job_title_text STRING NOT NULL OPTIONS(description="The title of a job position.")
, skill_mix_desc STRING NOT NULL OPTIONS(description="The classification of a job code.")
, auxiliary_status_code STRING NOT NULL OPTIONS(description="The status of an employee - FT, PT, PRN")
, normalized_skill_mix_desc STRING OPTIONS(description="The normalized classifications of a job code.")
, normalized_auxiliary_status_code STRING OPTIONS(description="The normalized status of an employee - FT, PT, PRN")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Job_Title_Text, Skill_Mix_Desc, Auxiliary_Status_Code
OPTIONS(description="Maps job code, status, and skill mix to a normalized value for RNs. Loaded from data provided by HRG.");
