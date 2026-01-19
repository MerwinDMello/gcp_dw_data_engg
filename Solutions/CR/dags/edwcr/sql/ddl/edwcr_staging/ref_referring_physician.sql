CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_referring_physician (
referring_physician_id INT64 NOT NULL
, referring_physician_name STRING
, navigation_referred_ind STRING
, biopsy_referred_ind STRING
, surgery_referred_ind STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
