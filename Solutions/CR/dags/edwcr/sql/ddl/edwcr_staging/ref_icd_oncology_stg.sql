CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_icd_oncology_stg (
icd_oncology_code STRING
, icd_oncology_type_code STRING
, icd_oncology_category_type_cd STRING
, icd_oncology_site_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
