create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_sector`
(
  sector_code STRING NOT NULL OPTIONS(description="unique list of sector values are maintained in this field."),
  sector_desc STRING OPTIONS(description="description of sector code is mainatined in this field"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY sector_code
OPTIONS(
  description="unique list of sector values are maintained in this table. there are corporate and field sector present in lawson."
);