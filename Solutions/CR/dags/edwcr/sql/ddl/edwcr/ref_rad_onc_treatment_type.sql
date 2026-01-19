CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_treatment_type
(
  treatment_type_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for treatment type in EDW'),
  treatment_category_desc STRING OPTIONS(description='Description for treatment category description'),
  treatment_type_desc STRING OPTIONS(description='Description for treatment type description'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY treatment_type_sk
OPTIONS(
  description='Contains information for radiation oncology treatment type'
);
