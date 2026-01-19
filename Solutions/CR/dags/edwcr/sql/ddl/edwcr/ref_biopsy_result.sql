CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_biopsy_result
(
  biopsy_result_id INT64 NOT NULL OPTIONS(description='A unique identifier for distinct biopsy results'),
  biopsy_result_desc STRING OPTIONS(description='The description tied to the biopsy result.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY biopsy_result_id
OPTIONS(
  description='Contains a distinct list of biopsy results outcomes.'
);
