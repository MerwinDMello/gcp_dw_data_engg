create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_comment_type`
AS SELECT
ref_comment_type.comment_type_code,
ref_comment_type.comment_type_desc,
ref_comment_type.source_system_code,
ref_comment_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_comment_type;