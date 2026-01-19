CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
(
  site_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a site in EDW'),
  source_site_id INT64 NOT NULL OPTIONS(description='Unique identifier for a site in Radiation Oncology'),
  source_site_guid_text STRING NOT NULL OPTIONS(description='Global unique identifier text for a site in Radiation Oncology'),
  site_code_text STRING NOT NULL OPTIONS(description='Text for site code'),
  site_name STRING NOT NULL OPTIONS(description='Name of the site'),
  server_name STRING OPTIONS(description='Name of the server for the site'),
  site_desc STRING OPTIONS(description='Description for site'),
  server_ip_address_text STRING OPTIONS(description='Text for server Internet Protocol address'),
  aura_version_text STRING OPTIONS(description='Text for Aura Version number'),
  aura_last_installed_date_time DATETIME OPTIONS(description='Date time when last installed'),
  registration_date_time DATETIME OPTIONS(description='Date time when site was registerd'),
  history_user_name STRING OPTIONS(description='Name of the user who updated record'),
  history_date_time DATETIME OPTIONS(description='Date time when record was updated'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY source_site_id
OPTIONS(
  description='Contains information for radiation oncology sites'
);
