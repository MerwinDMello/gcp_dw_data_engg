CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_procedure_type
(
  procedure_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each procedure type.'),
  procedure_type_desc STRING OPTIONS(description='The description for each procedure type.'),
  procedure_sub_type_desc STRING OPTIONS(description='Procedure sub type for Line Placement,Other Surgery and Other procedure.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY procedure_type_id
OPTIONS(
  description='Contains a distinct list of procedure types'
);
