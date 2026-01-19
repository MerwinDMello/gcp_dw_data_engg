CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_workforce_planning_target AS SELECT
    ref_workforce_planning_target.division_name,
    ref_workforce_planning_target.measure_name,
    ref_workforce_planning_target.period_end_date,
    ref_workforce_planning_target.metric_cnt,
    ref_workforce_planning_target.source_system_code,
    ref_workforce_planning_target.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_workforce_planning_target
;
