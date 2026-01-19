create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_sector_stg`
(
  sector_code STRING,
  sector_desc STRING
)
