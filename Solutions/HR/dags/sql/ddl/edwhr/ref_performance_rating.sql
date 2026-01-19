CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_performance_rating
(
  performance_rating_id INT64 NOT NULL OPTIONS(description="This field maintains list of ratings given by manager to empoyee or employee to self."),
  performance_rating_desc STRING OPTIONS(description="This field maintains performance rating description"),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
)
CLUSTER BY performance_rating_id
OPTIONS(description="This table maintains list of all self rating or manager rating codes and description.");