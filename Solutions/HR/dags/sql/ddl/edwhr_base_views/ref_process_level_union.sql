CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_process_level_union AS SELECT
    ref_process_level_union.process_level_code,
    ref_process_level_union.union_code,
    ref_process_level_union.union_name,
    ref_process_level_union.source_system_code,
    ref_process_level_union.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_process_level_union
;