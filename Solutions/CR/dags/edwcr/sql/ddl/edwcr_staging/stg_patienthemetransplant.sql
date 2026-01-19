CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_patienthemetransplant (
patienthemefactid INT64
, patientdimid INT64
, tumortypedimid INT64
, diagnosisresultid INT64
, diagnosisdimid INT64
, facilitydimid INT64
, coid STRING
, navigatordimid INT64
, transplantcandidacystatus STRING
, statusdate STRING
, candidacycomment STRING
, type_type STRING
, transplantdate STRING
, medicalspecialistdimid INT64
, transplantcomments STRING
, hbsource STRING
, transferdate STRING
, dw_last_update_date_time DATETIME
)
  ;
