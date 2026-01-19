CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_physician
(
  physician_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for each physician in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_physician_id INT64 NOT NULL OPTIONS(description='Unique identifier for a physician in Radiation Oncology in source'),
  location_sk INT64 OPTIONS(description='Unique key for a location'),
  resource_type_id INT64 OPTIONS(description='Identifier for resource type'),
  physician_first_name STRING OPTIONS(description='First name of the physician'),
  physician_last_name STRING OPTIONS(description='Last name of the physician'),
  physician_suffix_name STRING OPTIONS(description='Suffix name of the physician'),
  physician_alias_name STRING OPTIONS(description='Alias name of the physician'),
  physician_title_name STRING OPTIONS(description='Title name of the physician'),
  physician_email_address_text STRING OPTIONS(description='Email Address of the physician'),
  physician_specialty_text STRING OPTIONS(description='Text for physician specialty'),
  physician_id_text STRING OPTIONS(description='Text for physician identifier'),
  resource_active_ind STRING OPTIONS(description='Indicator which shows resource is active or deleted'),
  appointment_schedule_ind STRING OPTIONS(description='Indicates whether appointment can be scheduled or not'),
  physician_start_date_time DATETIME OPTIONS(description='Date time when physician started'),
  physician_termination_date_time DATETIME OPTIONS(description='Date time when physician terminated'),
  physician_institution_text STRING OPTIONS(description='Text for the institution with which physician is linked'),
  physician_comment_text STRING OPTIONS(description='Comment for physician'),
  resource_id INT64 OPTIONS(description='Internal Identification for Aria Resource (physician/staff/machine)'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_physician_id
OPTIONS(
  description='Contains information of physicians involved in Radiation Oncology'
);
