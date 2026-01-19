CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_factcoursediagnosis (
dimsiteid INT64
, factcoursediagnosisid INT64
, dimcourseid INT64
, factpatientdiagnosisid INT64
, dimdiagnosiscodeid INT64
, dimdxsiteid_bodysystem INT64
, dimdxsiteid INT64
, isprimary INT64
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
