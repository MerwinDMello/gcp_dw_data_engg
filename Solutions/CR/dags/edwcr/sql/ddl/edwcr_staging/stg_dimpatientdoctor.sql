CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimpatientdoctor (
dimsiteid INT64
, dimpatientdoctorid INT64
, dimpatientid INT64
, primaryflag INT64
, oncologistflag INT64
, ctrpatientser INT64
, ctrresourceser INT64
, ctrpt_id STRING
, ctrprovider_stkh_id STRING
, ctrorg_stkh_id STRING
, ctrpt_provider_id INT64
, referraldate STRING
, referralcode STRING
, profrelationtype INT64
, profrelationtypedesc STRING
, effectivestartdate STRING
, effectiveenddate STRING
, internalindicator STRING
, endreasoncode STRING
, activeentryindicator STRING
, validentryindicator STRING
, moroindicator STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
