/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_workforce_planning_target AS SELECT
      a.division_name,
      a.measure_name,
      a.period_end_date,
      a.metric_cnt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_workforce_planning_target AS a
  ;

