create table if not exists `{{ params.param_hr_stage_dataset_name }}.ref_comment_type_stg`
(
  comment_type_code STRING,
  comment_type_desc STRING
)
