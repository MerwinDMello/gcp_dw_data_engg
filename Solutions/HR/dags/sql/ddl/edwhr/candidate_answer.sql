CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.candidate_answer
(
answer_sid INT64 NOT NULL OPTIONS(description="Unique system generated identifier. Its a combination of answer number and source system code")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, answer_num INT64 NOT NULL OPTIONS(description="Unique answer number")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, answer_desc STRING OPTIONS(description="Answer description")
, question_sid INT64 OPTIONS(description="Unique question system identifier")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY answer_sid
OPTIONS(description="This table store the answers to the candidate question");
