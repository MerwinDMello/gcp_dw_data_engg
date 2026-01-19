CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_genetics_testing_stg (
geneticstestingfactid INT64
, corerecordid INT64
, patientdimid INT64
, tumortypedimid INT64
, diagnosisresultid INT64
, diagnosisdimid INT64
, facilitydimid INT64
, coid STRING NOT NULL
, navigatordimid INT64
, geneticsdate DATE
, geneticstesttype STRING
, geneticsspecialist STRING
, geneticsbrcatype STRING
, geneticscomments STRING
, hbsource STRING
, dw_last_update_date_time DATETIME
)
  ;
