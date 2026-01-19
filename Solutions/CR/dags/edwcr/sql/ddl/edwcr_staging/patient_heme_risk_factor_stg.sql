CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.patient_heme_risk_factor_stg (
patienthemediagnosisfactid INT64 NOT NULL
, patientdimid NUMERIC(18,0)
, tumortypedimid INT64
, diagnosisresultid INT64
, diagnosisdimid INT64
, facilitydimid INT64
, coid STRING NOT NULL
, navigatordimid NUMERIC(29,0)
, riskfactor STRING
, otherriskfactor STRING
, tumordiseasesite STRING
, othertumordiseasesite STRING
, hashbite_ssk STRING
, dw_last_update_date_time DATETIME
)
  ;
