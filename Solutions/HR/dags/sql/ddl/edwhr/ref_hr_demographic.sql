create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_hr_demographic`
(
  demographic_code STRING NOT NULL OPTIONS(description="demographic code that is ethncity/race/veteran status."),
  demographic_type_code STRING NOT NULL OPTIONS(description="type of demographic code. veteran status or ethnicity"),
  active_flag STRING OPTIONS(description="y/n character to indicate this record as active in the edw."),
  demographic_desc STRING OPTIONS(description="description of the demographic codes."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY demographic_code, demographic_type_code
OPTIONS(
  description="contains the description to various hr demographic codes include race/ethnicity/veteran status."
);