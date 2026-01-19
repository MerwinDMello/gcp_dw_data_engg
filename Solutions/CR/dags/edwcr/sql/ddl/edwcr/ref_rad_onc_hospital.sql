CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_hospital
(
  hospital_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a radiation oncology hospital data in EDW'),
  hospital_address_sk INT64 OPTIONS(description='Unique key for hospital address'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_hospital_id INT64 NOT NULL OPTIONS(description='Unique identifier for Radiation Oncology hospital'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  hospital_name STRING OPTIONS(description='Name of the hospital'),
  hospital_email_address_text STRING OPTIONS(description='Text for hospital email address'),
  hospital_history_user_name STRING OPTIONS(description='Name of the user who updated hospital record'),
  hospital_history_date_time DATETIME OPTIONS(description='Date time when hospital record was updated'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_hospital_id
OPTIONS(
  description='Contains information for radiation oncology hospital'
);
