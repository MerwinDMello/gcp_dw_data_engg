CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_breast_cancer_type_stg (
breast_cancer_type_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
