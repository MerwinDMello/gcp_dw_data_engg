CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_competency_group
AS SELECT
    ref_competency_group.competency_group_id,
    ref_competency_group.competency_group_desc,
    ref_competency_group.source_system_code,
    ref_competency_group.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_competency_group;