create table if not exists {{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk (
workunit_sid numeric(18,0) not null
, activity_seq_num int64 not null
, valid_from_date datetime not null
, valid_to_date datetime
, workunit_num numeric(12,0) not null
, start_date_time datetime
, end_date_time datetime
, activity_name string
, activity_type_text string
, action_taken_text string
, node_caption_text string
, user_profile_id_text string
, authenticated_author_text string
, task_name string
, task_type_num int64
, lawson_company_num int64 not null
, process_level_code string not null
, active_dw_ind string not null
, source_system_code string not null
, dw_last_update_date_time datetime not null
)
  ;

