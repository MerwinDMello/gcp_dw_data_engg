CREATE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_leadership_level
AS SELECT
    ref_leadership_level.leadership_level_sid,
    ref_leadership_level.leadership_level_desc,
    ref_leadership_level.source_system_code,
    ref_leadership_level.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_leadership_level;