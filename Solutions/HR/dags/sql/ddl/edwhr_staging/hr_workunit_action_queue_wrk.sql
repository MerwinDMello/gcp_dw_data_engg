CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.hr_workunit_action_queue_wrk (
workunit_sid NUMERIC(18,0) NOT NULL
, activity_seq_num INT64 NOT NULL
, valid_from_date DATETIME NOT NULL
, valid_to_date DATETIME
, workunit_num NUMERIC(12,0) NOT NULL
, work_desc STRING
, action_taken_text STRING
, display_type_num INT64
, display_name STRING
, filter_key_text STRING
, filter_value_num_text STRING
, time_out_type_num INT64
, time_out_hour_num INT64
, time_out_action_text STRING
, timed_out_sw INT64
, last_queue_action_num INT64
, configurator_name STRING
, lawson_company_num INT64 NOT NULL
, process_level_code STRING NOT NULL
, active_dw_ind STRING NOT NULL
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
