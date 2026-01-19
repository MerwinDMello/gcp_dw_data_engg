CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_nursing_school_campus (
campus_id INT64 NOT NULL OPTIONS(description="This contains unique campus identifier associated with nursing school.")
, campus_name STRING NOT NULL OPTIONS(description="Contains name of the nursing campus")
, campus_code STRING OPTIONS(description="Contains unique nursing campus code")
, nursing_school_id INT64 NOT NULL OPTIONS(description="Unique nursing school identifier")
, addr_sid INT64 OPTIONS(description="Unique system genderated address identifier")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Campus_Id
OPTIONS(description="This table contains campus details associated to nursing school.");