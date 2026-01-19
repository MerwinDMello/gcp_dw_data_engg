CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_pathway_var_reason
(
  pathway_var_reason_id INT64 NOT NULL OPTIONS(description='A unique identifier for variance reason if the patient is not compliant to a pathway'),
  pathway_var_reason_type_desc STRING OPTIONS(description='Consist of variance reason if the patient is not compliant to a pathway'),
  pathway_var_reason_sub_type_desc STRING OPTIONS(description='Sub type for variance reason if the patient is not compliant to a pathway'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY pathway_var_reason_id
OPTIONS(
  description='Contains a distinct list of pathway variance reason'
);
