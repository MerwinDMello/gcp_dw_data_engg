CREATE TABLE IF NOT EXISTS `{{ params.param_hr_core_dataset_name }}.ref_clock`
(
  clock_code STRING NOT NULL OPTIONS(description="Code for each clock library. Each clock code rolls up to a clock library."),
  clock_library_code STRING NOT NULL OPTIONS(description="Code for each clock library a clock code can belong to."),
  clock_desc STRING OPTIONS(description="Description for each clock code."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY clock_code, clock_library_code
OPTIONS(
  description="This table contains the various clocks that an employee can charge their time to."
);