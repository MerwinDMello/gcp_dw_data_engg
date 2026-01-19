CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_staging_sample_stage (
patientid INT64 NOT NULL
, tumorid INT64 NOT NULL
, cancer_stge_clsfctn_mthd_cde STRING
, cancer_stage_type_code STRING
, clin_t_tnm STRING
, ajccstagegroupclin STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
, code STRING
, sub INT64
, group1 INT64
)
  ;
