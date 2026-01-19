CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_profile_medium
(
  profile_medium_id INT64 NOT NULL OPTIONS(description="A unique identifier for the medium in which a profile was submitted to HCA."),
  profile_medium_code STRING OPTIONS(description="A short description of the medium in which a profile was submitted to HCA."),
  profile_medium_desc STRING OPTIONS(description="A description of the medium in which a profile was submitted to HCA."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY profile_medium_id
OPTIONS(
  description="Table contains a list of different mediums that an application can be submitted."
);