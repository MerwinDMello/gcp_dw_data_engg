CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_pat_tumor_pathology_result_stg (
tumorid INT64 NOT NULL
, patientid INT64 NOT NULL
, nodesexamined STRING
, nodespositive STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
