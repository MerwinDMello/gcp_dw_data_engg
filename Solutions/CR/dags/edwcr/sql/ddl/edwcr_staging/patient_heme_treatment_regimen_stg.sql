CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.patient_heme_treatment_regimen_stg (
patienthemediagnosisfactid NUMERIC(29,0)
, patientdimid NUMERIC(29,0)
, tumortypedimid NUMERIC(29,0)
, diagnosisresultid NUMERIC(29,0)
, diagnosisdimid NUMERIC(29,0)
, facilitydimid NUMERIC(29,0)
, coid STRING
, navigatordimid NUMERIC(29,0)
, regimen STRING
, treatmentphase STRING
, plannedstartdate STRING
, actualstartdate STRING
, drugs STRING
, cycles INT64
, cyclelength STRING
, cyclelengthinterval STRING
, cyclenumber STRING
, pathwayyesno STRING
, pathwayname STRING
, pathwaycompliant STRING
, pathwayvariancereason STRING
, otherpathwayreason STRING
, treatmentplandoccumentdate STRING
, plandocumentedtimeframe STRING
, treatmentregimencoments STRING
, hbsource STRING
, lastupdatedatetime STRING
)
  ;
