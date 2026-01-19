CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_disease_assess_source
(
  disease_assess_source_id INT64 NOT NULL OPTIONS(description='A unique identifier for each disease assessment source value'),
  disease_assess_source_name STRING OPTIONS(description='Name for source of the Disease Assessment'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY disease_assess_source_id
OPTIONS(
  description='Contains the list of source for disease assessment'
);
