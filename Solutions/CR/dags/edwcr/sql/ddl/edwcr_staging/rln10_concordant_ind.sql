CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.rln10_concordant_ind (
patientid INT64
, tumorid INT64
, rln10_concordant_ind STRING
, dw_last_update_date_time DATETIME
)
  ;
