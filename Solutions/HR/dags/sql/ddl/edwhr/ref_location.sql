create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_location`
(
  location_code STRING NOT NULL OPTIONS(description="unique list of location codes are maintained in this table. location codes are mainatined for employee working location and also for process level location."),
  location_desc STRING OPTIONS(description="this represents the description associated with a employee or postion or requisition location"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY location_code
OPTIONS(
  description="unique list of process level, employee working locations codes maintained in this table."
);