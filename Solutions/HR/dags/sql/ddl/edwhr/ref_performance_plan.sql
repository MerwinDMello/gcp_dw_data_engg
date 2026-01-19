CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_performance_plan
(
  performance_plan_id INT64 NOT NULL OPTIONS(description="This field maintain unique sequence number generated for each unique plan name"),
  performance_plan_desc STRING NOT NULL OPTIONS(description="This table maintain description of plan"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY performance_plan_id
OPTIONS(description="This table maintain list of unique performance plan against which goal is created. Different performance plans are created on executive level, staff level or leader level.");