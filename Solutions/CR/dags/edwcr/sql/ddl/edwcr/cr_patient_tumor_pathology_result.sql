CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_tumor_pathology_result
(
  tumor_id INT64 NOT NULL OPTIONS(description='Unique identifier of tumor'),
  cr_patient_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  nodes_examined_id INT64 OPTIONS(description='Unique identifier for nodes assessed'),
  positive_node_id INT64 OPTIONS(description='Unique identifier for positive nodes'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY tumor_id, cr_patient_id
OPTIONS(
  description='This table contains pathology results of patient tumor'
);
