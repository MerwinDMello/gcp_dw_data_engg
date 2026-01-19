/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.requisition_workflow AS SELECT
      a.requisition_sid,
      a.workflow_seq_num,
      a.valid_from_date,
      a.valid_to_date,
      a.workflow_workunit_num_text,
      a.workflow_activity_num,
      a.workflow_role_name,
      a.workflow_task_name,
      a.start_date,
      a.start_time,
      a.end_date,
      a.end_time,
      a.workflow_user_id_code,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition_workflow AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

