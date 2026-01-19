CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_national_svc_center_stg (
id INT64
, code STRING NOT NULL
, subcode STRING NOT NULL
, description STRING NOT NULL
, category STRING NOT NULL
, subcategory STRING NOT NULL
, dw_last_update_date_time DATETIME
)
  ;
