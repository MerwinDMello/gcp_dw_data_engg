CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_functional_assess_stg (
patienthemefuncassessfactid INT64
, patientdimid INT64
, tumortypedimid INT64
, diagnosisresultid INT64
, diagnosisdimid INT64
, facilitydimid INT64
, coid STRING
, navigatordimid INT64
, functionalassessmentdate DATE
, testtype STRING
, testtyperesults NUMERIC(18,0)
, hbsource STRING
, source_lastupdatedatetime DATETIME
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
