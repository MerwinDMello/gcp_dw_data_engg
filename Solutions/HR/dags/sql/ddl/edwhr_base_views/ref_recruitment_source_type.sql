create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type`
AS SELECT
    ref_recruitment_source_type.recruitment_source_type_id,
    ref_recruitment_source_type.recruitment_source_type_code,
    ref_recruitment_source_type.recruitment_source_type_desc,
    ref_recruitment_source_type.source_system_code,
    ref_recruitment_source_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_recruitment_source_type;