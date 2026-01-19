CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_location
(
  location_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a radiation oncology location data in EDW'),
  site_sk INT64 NOT NULL OPTIONS(description='Unique identifier for a site of Radiation oncology'),
  source_location_id INT64 NOT NULL OPTIONS(description='Unique identifier for Radiation Oncology location'),
  country_name STRING OPTIONS(description='Name of the country'),
  state_name STRING OPTIONS(description='Name of the state'),
  city_name STRING OPTIONS(description='Name of the city'),
  county_name STRING OPTIONS(description='Name of the county'),
  zip_code STRING OPTIONS(description='Zip code for the location'),
  log_id INT64 OPTIONS(description='Unique identifier for the log'),
  run_id INT64 OPTIONS(description='Identifier for last run'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_sk, source_location_id
OPTIONS(
  description='Contains information for radiation oncology location'
);
