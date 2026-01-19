CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.ref_leadership_level
(
  leadership_level_sid INT64 NOT NULL OPTIONS(description="This field mainatins unique list of ETL generated unique sequence number for each roles available in HCA"),
  leadership_level_desc STRING OPTIONS(description="This field maintains description of future role"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY leadership_level_sid
OPTIONS(description="This table maintains list of all leadership levels maintained in employee performance management system");