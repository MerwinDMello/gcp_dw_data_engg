CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_tumor_site
(
  tumor_site_id INT64 NOT NULL OPTIONS(description='Identifier for the tumor site.'),
  tumor_site_desc STRING OPTIONS(description='Location of the tumor based on Diagnosis Code'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY tumor_site_id
OPTIONS(
  description='Site of the tumor.'
);
