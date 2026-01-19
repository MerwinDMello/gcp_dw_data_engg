CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_posting_board_type
(
posting_board_type_id INT64 NOT NULL OPTIONS(description="Unique Identifier for each board type.")
, posting_board_type_code STRING OPTIONS(description="This code indicates the posting board type like external , intranet etc.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY posting_board_type_id
OPTIONS(description="This table maintains the reference data for Posting Board type like External, Intranet etc.");