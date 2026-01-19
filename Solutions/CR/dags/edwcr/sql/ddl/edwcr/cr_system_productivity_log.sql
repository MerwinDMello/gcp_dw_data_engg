CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_system_productivity_log
(
  system_productivity_log_id INT64 NOT NULL OPTIONS(description='Unique identifier for System productivity log'),
  cr_patient_id INT64 OPTIONS(description='A unique identifier for each patient.'),
  tumor_id INT64 OPTIONS(description='Unique identifier of tumor'),
  system_user_id_code STRING OPTIONS(description='Identifier of person logged into the system at time of patient information access'),
  system_change_status_date DATE OPTIONS(description='System Generated date of the change of status to Complete'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY system_productivity_log_id
OPTIONS(
  description='Contains system productivity log details'
);
