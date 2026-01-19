CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_site_location
(
  site_location_id INT64 NOT NULL OPTIONS(description='A unique identifier for each site.'),
  site_location_desc STRING OPTIONS(description='The description for each distinct site.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY site_location_id
OPTIONS(
  description='Contains a distinct list of treatment or biopsy sites.'
);
