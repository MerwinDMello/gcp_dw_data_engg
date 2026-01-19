CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_degree_rank (
degree_rank_level_num INT64 NOT NULL OPTIONS(description="This field captures degree rank of different education levels.")
, subject_code STRING NOT NULL OPTIONS(description="This field captures subject code.")
, education_code STRING OPTIONS(description="This field maintains degree code of the education level.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Degree_Rank_Level_Num, Subject_Code
OPTIONS(description="This table maintains degree rank information by different subjects");
