CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.dimdiagnosiscode_stg (
dimsiteid INT64
, dimdiagnosiscodeid INT64
, diagnosiscode STRING
, diagnosiscodeclsschemeid INT64
, diagnosisclinicaldescriptionen STRING
, diagnosisfulltitleenu STRING
, diagnosistableenu STRING
, logid INT64
, runid INT64
, dw_last_update_date_time DATETIME
)
  ;
