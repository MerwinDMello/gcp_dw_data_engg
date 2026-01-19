CREATE TABLE {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute
(
  future_role_attribute_id INT64 NOT NULL OPTIONS(description="This field mainatins unique list of ETL generated unique sequence number for each roles available in HCA"),
  future_role_attribute_desc STRING NOT NULL OPTIONS(description="This field maintains description of future role"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY future_role_attribute_id
OPTIONS( description="This table maintains list of all leadership levels maintained in employee performance management system and possible role attributes");