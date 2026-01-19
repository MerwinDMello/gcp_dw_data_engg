CREATE TABLE IF NOT EXISTS `{{ params.param_hr_core_dataset_name }}.ref_exception`
(
  exception_code STRING NOT NULL OPTIONS(description="Designates the type of exception a time record can have."),
  exception_desc STRING OPTIONS(description="Describes the type of exception."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY exception_code
OPTIONS(
  description="Contains the exception codes used for each time record."
);