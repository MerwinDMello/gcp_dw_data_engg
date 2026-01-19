CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code
(
  procedure_code_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a procedure code in EDW'),
  treatment_type_sk INT64 OPTIONS(description='Unique surrogate key generated for treatment type in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_procedure_code_id INT64 NOT NULL OPTIONS(description='Unique identifier for a procedure code in Radiation Oncology'),
  procedure_code STRING OPTIONS(description='Procedure or billing code for radiation oncology'),
  procedure_short_desc STRING OPTIONS(description='Short (abbreviated) procedure description'),
  procedure_long_desc STRING OPTIONS(description='Long description for procedure or billing code'),
  active_ind STRING OPTIONS(description='Indicator which shows that record is active or not'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_procedure_code_id
OPTIONS(
  description='Contains information for radiation oncology procedure code'
);
