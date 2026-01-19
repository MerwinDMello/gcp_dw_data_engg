CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_question_type (
question_type_code STRING NOT NULL OPTIONS(description="It captures type of each individual survey question. Question types are Single Selection, Multi selection, free comment text etc.")
, question_type_desc STRING OPTIONS(description="Description of question type code")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY question_type_code
OPTIONS(description="This table contains unique list of question types.");
