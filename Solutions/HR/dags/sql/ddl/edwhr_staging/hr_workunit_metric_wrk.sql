CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.hr_workunit_metric_wrk (
workunit_sid NUMERIC(18,0) NOT NULL
, activity_seq_num INT64 NOT NULL
, user_profile_id_text STRING NOT NULL
, valid_from_date DATETIME NOT NULL
, valid_to_date DATETIME
, workunit_num NUMERIC(12,0) NOT NULL
, task_name STRING
, task_type_num INT64 NOT NULL
, queue_assigment_num INT64
, action_start_date_time DATETIME
, action_taken_text STRING
, comment_text STRING
, authenticated_author_text STRING
, lawson_company_num INT64 NOT NULL
, process_level_code STRING NOT NULL
, active_dw_ind STRING NOT NULL
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;

