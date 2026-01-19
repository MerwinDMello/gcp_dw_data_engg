CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk (
workunit_sid NUMERIC(18,0) NOT NULL
, valid_from_date DATETIME NOT NULL
, valid_to_date DATETIME
, workunit_num NUMERIC(12,0) NOT NULL
, application_data_area_text STRING
, application_key_text STRING
, application_value_text STRING
, service_definition_text STRING
, flow_definition_text STRING
, flow_version_num INT64
, actor_id_text STRING
, authenticated_actor_text STRING
, work_title_text STRING
, filter_key_text STRING
, filter_value_text STRING
, execution_start_date_time DATETIME
, start_date_time DATETIME
, close_date_time DATETIME
, status_id INT64
, criterion_1_id_text STRING
, criterion_2_id_text STRING
, criterion_3_id_text STRING
, source_type_text STRING
, source_1_text STRING
, configuration_name STRING
, last_activity_num INT64
, last_message_num INT64
, last_variable_seq_num INT64
, classic_workunit_id INT64
, classic_workunit_entered_ind STRING
, key_field_value_1_text STRING
, key_field_value_2_text STRING
, key_field_value_3_text STRING
, key_field_value_4_text STRING
, key_field_value_5_text STRING
, key_field_value_6_text STRING
, key_field_value_7_text STRING
, key_field_value_8_text STRING
, key_field_value_9_text STRING
, key_field_value_10_text STRING
, lawson_company_num INT64 NOT NULL
, process_level_code STRING NOT NULL
, active_dw_ind STRING NOT NULL
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
