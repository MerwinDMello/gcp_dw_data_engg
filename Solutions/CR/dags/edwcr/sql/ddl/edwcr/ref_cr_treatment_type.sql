CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_cr_treatment_type
(
  treatment_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for type of treatment'),
  treatment_type_code STRING OPTIONS(description='Alpha character representation of the treatment performed'),
  treatment_type_desc STRING OPTIONS(description='Description of Treatment performed'),
  treatment_group_id INT64 OPTIONS(description='Group Identifier for a treatment'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY treatment_type_id
OPTIONS(
  description='This table contains different codes and descriptions for treatments'
);
