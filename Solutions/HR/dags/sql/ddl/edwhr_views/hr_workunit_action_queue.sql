/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_workunit_action_queue AS SELECT
      a.workunit_sid,
      a.activity_seq_num,
      a.valid_from_date,
      a.valid_to_date,
      a.workunit_num,
      a.work_desc,
      a.action_taken_text,
      a.display_type_num,
      a.display_name,
      a.filter_key_text,
      a.filter_value_num_text,
      a.time_out_type_num,
      a.time_out_hour_num,
      a.time_out_action_text,
      a.timed_out_sw,
      a.last_queue_action_num,
      a.configurator_name,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_workunit_action_queue AS a
  ;

