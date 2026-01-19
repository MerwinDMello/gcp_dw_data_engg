CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_question
(
question_sid INT64 NOT NULL OPTIONS(description="This is a unique system generated identifier. Its a combination of question number and source system code")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, question_num INT64 NOT NULL OPTIONS(description="Unique question number")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, creation_date DATE OPTIONS(description="Question creation date")
, question_desc STRING OPTIONS(description="Question that is asked to candidate.")
, question_code STRING OPTIONS(description="Question code")
, last_modified_date DATE OPTIONS(description="Last modified date")
, requisition_num INT64 OPTIONS(description="A unique number for each requisition coming from the source.")
, question_type_num INT64 OPTIONS(description="Unique question type number")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY question_sid
OPTIONS(description="This table stores the questions that are asked to candidate.");