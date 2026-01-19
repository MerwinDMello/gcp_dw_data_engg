CREATE TABLE {{ params.param_hr_core_dataset_name }}.ref_performance_status
(
  performance_status_id INT64 NOT NULL OPTIONS(description="This field maintains unique list of ETL generated sequence number for each unique performance status description"),
  performance_status_desc STRING NOT NULL OPTIONS(description="This field maintains description of performance status code"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY performance_status_id
OPTIONS(description="Contains details around the candidate based on user-defined fields in the source.");