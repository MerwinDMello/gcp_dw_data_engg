CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_stg (
patienthemefactid INT64
, patientdimid INT64
, tumortypedimid INT64
, diagnosisresultid INT64
, diagnosisdimid INT64
, facilitydimid INT64
, coid STRING
, navigatordimid INT64
, housing STRING
, localhousingaddress STRING
, otherlocalhousingaddress STRING
, transportation STRING
, drugusehistory STRING
, hematologist STRING
, hbsource STRING
, lastupdatedatetime STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
