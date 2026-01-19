BEGIN

DECLARE current_ts datetime;
DECLARE DUP_COUNT INT64; 
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;


TRUNCATE TABLE {{params.param_hr_stage_dataset_name}}.hr_workunit_action_queue_wrk;

  INSERT INTO {{params.param_hr_stage_dataset_name}}.hr_workunit_action_queue_wrk (workunit_sid, activity_seq_num, valid_from_date, valid_to_date, workunit_num, work_desc, action_taken_text, display_type_num, display_name, filter_key_text, filter_value_num_text, time_out_type_num, time_out_hour_num, time_out_action_text, timed_out_sw, last_queue_action_num, configurator_name, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid AS workunit_sid,
        pfiactivity AS activity_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        pfiworkunit AS workunit_num,
        workdescription AS work_desc,
        actiontaken AS action_taken_text,
        displaytype AS display_type_num,
        displayname AS display_name,
        pfifilterkey AS filter_key_text,
        filtervalue AS filter_value_num_text,
        timeouttype AS time_out_type_num,
        CAST(timeouthour AS INT64) AS time_out_hour_num,
        timeoutaction AS time_out_action_text,
        CAST(timedout AS INT64) AS timed_out_sw,
        lastqueueaction AS last_queue_action_num,
        configname AS configurator_name,
        b.lawson_company_num AS lawson_company_num,
        b.process_level_code AS process_level_code,
        'Y' AS active_dw_ind,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{params.param_hr_stage_dataset_name}}.pfiqueue AS a
        INNER JOIN {{params.param_hr_base_views_dataset_name}}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'L'
         AND b.valid_to_date = DATETIME("9999-12-31 23:59:59");

  INSERT INTO {{params.param_hr_stage_dataset_name}}.hr_workunit_action_queue_wrk (workunit_sid, activity_seq_num, valid_from_date, valid_to_date, workunit_num, work_desc, action_taken_text, display_type_num, display_name, filter_key_text, filter_value_num_text, time_out_type_num, time_out_hour_num, time_out_action_text, timed_out_sw, last_queue_action_num, configurator_name, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid AS workunit_sid,
        pfiactivity AS activity_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        pfiworkunit AS workunit_num,
        workdescription AS work_desc,
        actiontaken AS action_taken_text,
        displaytype AS display_type_num,
        displayname AS display_name,
        pfifilterkey AS filter_key_text,
        filtervalue AS filter_value_num_text,
        timeouttype AS time_out_type_num,
        cast(timeouthour as int64) AS time_out_hour_num,
        timeoutaction AS time_out_action_text,
        timedout AS timed_out_sw,
        lastqueueaction AS last_queue_action_num,
        configname AS configurator_name,
        b.lawson_company_num AS lawson_company_num,
        b.process_level_code AS process_level_code,
        'Y' AS active_dw_ind,
        'B' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{params.param_hr_stage_dataset_name}}.ats_cust_pfiqueue_stg AS a
        INNER JOIN {{params.param_hr_base_views_dataset_name}}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'B'
         AND b.valid_to_date = DATETIME("9999-12-31 23:59:59")
      QUALIFY row_number() OVER (PARTITION BY workunit_sid, activity_seq_num ORDER BY a.repset_variation_id DESC) = 1;

END;
