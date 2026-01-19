CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_performance_category
(
  performance_category_id INT64 NOT NULL OPTIONS(description="This field maintain list of categories on which employees performance goal is maintained."),
  performance_category_desc STRING NOT NULL OPTIONS(description="This field maintain description of performance category"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY performance_category_id
OPTIONS(description="This table maintains list of all Categories and description on which employees performance goal is created");