CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_tumor_stage (
code STRING
, ajccstagegrouppath_desc STRING
, sub INT64
, group1 INT64
, tumorid1 INT64 NOT NULL
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
