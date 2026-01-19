CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2 (
hca_unique STRING
, surv_id NUMERIC
, adj_samp STRING
, survey_type STRING
, pg_unit STRING
, disdate DATE
, recdate DATE
, mde STRING
, question_id STRING
, coid STRING
, resp STRING
, ptype STRING
, qtype STRING
, category STRING
, dw_last_update_date_time DATETIME
, flag STRING
, cms_rpt STRING
)
  ;
