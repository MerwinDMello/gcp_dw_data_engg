CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_detail_tumor_site
(
  detail_tumor_site_id INT64 NOT NULL OPTIONS(description='Identifier for the detailed tumor site.'),
  detail_tumor_site_desc STRING OPTIONS(description='Detailed location of the tumor based on Diagnosis Code'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY detail_tumor_site_id
OPTIONS(
  description='Reference for the detail location of the tumor site.'
);
