CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_question_answer
(
  question_answer_sid INT64 NOT NULL OPTIONS(description="Unique question answer system identifier. Its a combination of question answer num and source system code"),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded."),
  question_answer_num INT64 OPTIONS(description="Unique question answer number"),
  candidate_sid INT64 OPTIONS(description="Unique candidate system identifier"),
  valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded."),
  creation_date DATE OPTIONS(description="Creation date"),
  question_sid INT64 OPTIONS(description="Unique question system identifier"),
  answer_sid INT64 OPTIONS(description="Unique answer system identifier"),
  comment_text STRING OPTIONS(description="Comment related to answer"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY question_answer_sid, valid_from_date
OPTIONS(
  description="This table stores the candidate, question and answer related to recruitment"
);