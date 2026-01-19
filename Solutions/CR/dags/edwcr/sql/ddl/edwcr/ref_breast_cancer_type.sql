CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_breast_cancer_type
(
  breast_cancer_type_id INT64 NOT NULL OPTIONS(description='Identifier indicating which Breast Cancer gene mutation was tested for.'),
  breast_cancer_type_desc STRING OPTIONS(description='Breast Cancer gene mutation description'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY breast_cancer_type_id
OPTIONS(
  description='Contains the Breast Cancer type codes'
);
