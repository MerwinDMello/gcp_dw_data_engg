CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_microstep (
microstep_num INT64 NOT NULL OPTIONS(description="Unique microstep number for microstep reporting")
, microstep_name STRING OPTIONS(description="Unique microstep name for microstep reporting")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
 CLUSTER BY Microstep_Num
OPTIONS(description="This table maintains unique microstep number and name for microstep reporting");
