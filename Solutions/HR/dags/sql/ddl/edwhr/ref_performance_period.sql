CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_performance_period
(
  review_period_id INT64 NOT NULL OPTIONS(description="It is ETL generated sequence number for unique Period for which the review was executed"),
  review_period_desc STRING NOT NULL OPTIONS(description="This field maintains description for review period"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY review_period_id
OPTIONS(description="This table maintains ");