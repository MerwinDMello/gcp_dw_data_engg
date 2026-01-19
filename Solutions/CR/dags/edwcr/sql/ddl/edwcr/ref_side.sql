CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_side
(
  side_id INT64 NOT NULL OPTIONS(description='A unique identifier for each side.'),
  side_desc STRING OPTIONS(description='The description for the side.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY side_id
OPTIONS(
  description='Contains a distinct list of sides different treatment types could take place.'
);
