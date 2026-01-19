/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.hr_workunit_activity AS SELECT
    hr_workunit_activity.workunit_sid,
    hr_workunit_activity.activity_seq_num,
    hr_workunit_activity.valid_from_date,
    hr_workunit_activity.valid_to_date,
    hr_workunit_activity.workunit_num,
    hr_workunit_activity.start_date_time,
    hr_workunit_activity.end_date_time,
    hr_workunit_activity.activity_name,
    hr_workunit_activity.activity_type_text,
    hr_workunit_activity.action_taken_text,
    hr_workunit_activity.node_caption_text,
    hr_workunit_activity.user_profile_id_text,
    hr_workunit_activity.authenticated_author_text,
    hr_workunit_activity.task_name,
    hr_workunit_activity.task_type_num,
    hr_workunit_activity.lawson_company_num,
    hr_workunit_activity.process_level_code,
    hr_workunit_activity.active_dw_ind,
    hr_workunit_activity.source_system_code,
    hr_workunit_activity.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.hr_workunit_activity;