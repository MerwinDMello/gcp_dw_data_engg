create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_position_detail`
(
  position_detail_code STRING NOT NULL OPTIONS(description="this number describes the category of the position data (89 = wfh position)"),
  position_detail_code_desc STRING OPTIONS(description="describes the category of the field key id."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY position_detail_code
OPTIONS(
  description="this tables contains information about the position codes that link to position_detail."
);