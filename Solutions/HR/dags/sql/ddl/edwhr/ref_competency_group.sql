CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_competency_group
(
  competency_group_id INT64 NOT NULL OPTIONS(description="This field maintains unique sequence number of each description of competency group"),
  competency_group_desc STRING NOT NULL OPTIONS(description="This field maintains description of each competency group"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY competency_group_id
OPTIONS(description="This table maintains list of all competancy group available within HCA performance management system");