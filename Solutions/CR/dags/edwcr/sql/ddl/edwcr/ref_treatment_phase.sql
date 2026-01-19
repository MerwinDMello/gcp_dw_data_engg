CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_treatment_phase
(
  treatment_phase_id INT64 NOT NULL OPTIONS(description='A unique identifier for each treatment phase'),
  treatment_phase_desc STRING OPTIONS(description='Description of the treatment phase'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY treatment_phase_id
OPTIONS(
  description='Contains the distinct list of treatment phase'
);
