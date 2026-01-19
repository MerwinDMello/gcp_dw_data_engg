CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_candidate_question_type
(
  question_type_num INT64 NOT NULL OPTIONS(description="Unique question type number")
, question_type_desc STRING OPTIONS(description="Question type description")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY question_type_num
OPTIONS(description="This table stores unique candidate question type.");