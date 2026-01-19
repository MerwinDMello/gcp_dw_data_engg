CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_competency
AS SELECT
    ref_competency.competency_id,
    ref_competency.competency_desc,
    ref_competency.source_system_code,
    ref_competency.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_competency;