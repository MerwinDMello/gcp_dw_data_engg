/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_workunit_activity AS SELECT
      a.workunit_sid,
      a.activity_seq_num,
      a.valid_from_date,
      a.valid_to_date,
      a.workunit_num,
      a.start_date_time,
      a.end_date_time,
      a.activity_name,
      a.activity_type_text,
      a.action_taken_text,
      a.node_caption_text,
      a.user_profile_id_text,
      a.authenticated_author_text,
      a.task_name,
      a.task_type_num,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_workunit_activity AS a
  ;

