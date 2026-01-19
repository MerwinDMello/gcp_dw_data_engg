/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_workunit AS SELECT
      a.workunit_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.workunit_num,
      a.application_data_area_text,
      a.application_key_text,
      a.application_value_text,
      a.service_definition_text,
      a.flow_definition_text,
      a.flow_version_num,
      a.actor_id_text,
      a.authenticated_actor_text,
      a.work_title_text,
      a.filter_key_text,
      a.filter_value_text,
      a.execution_start_date_time,
      a.start_date_time,
      a.close_date_time,
      a.status_id,
      a.criterion_1_id_text,
      a.criterion_2_id_text,
      a.criterion_3_id_text,
      a.source_type_text,
      a.source_1_text,
      a.configuration_name,
      a.last_activity_num,
      a.last_message_num,
      a.last_variable_seq_num,
      a.classic_workunit_id,
      a.classic_workunit_entered_ind,
      a.key_field_value_1_text,
      a.key_field_value_2_text,
      a.key_field_value_3_text,
      a.key_field_value_4_text,
      a.key_field_value_5_text,
      a.key_field_value_6_text,
      a.key_field_value_7_text,
      a.key_field_value_8_text,
      a.key_field_value_9_text,
      a.key_field_value_10_text,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS a
  ;

