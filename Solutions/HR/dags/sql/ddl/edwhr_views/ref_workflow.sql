/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_workflow AS SELECT
      a.workflow_id,
      a.active_sw,
      a.workflow_code,
      a.workflow_name,
      a.workflow_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS a
  ;

