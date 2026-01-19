CREATE TABLE {{ params.param_hr_core_dataset_name }}.ref_nursing_school
(
  nursing_school_id INT64 NOT NULL OPTIONS(description="Unique nursing school identifier"),
  nursing_school_name STRING NOT NULL OPTIONS(description="Contains nursing school name"),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY nursing_school_id
OPTIONS(
    description="This table contains the list of unique nursing schools."
);

