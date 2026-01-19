/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_hiring_step_status_order AS SELECT
      a.step_status_order_id,
      a.step_name,
      a.submission_status_name,
      a.step_status_order_num,
      a.step_status_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_hiring_step_status_order AS a
  ;

