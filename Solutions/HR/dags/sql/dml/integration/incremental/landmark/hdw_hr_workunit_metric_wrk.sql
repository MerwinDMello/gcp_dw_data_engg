BEGIN

DECLARE current_ts datetime;
SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hr_workunit_metric_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_workunit_metric_wrk (workunit_sid, activity_seq_num, user_profile_id_text, valid_from_date, valid_to_date, workunit_num, task_name, task_type_num, queue_assigment_num, action_start_date_time, action_taken_text, comment_text, authenticated_author_text, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid AS workunit_sid,
        pfiactivity AS activity_seq_num,
        TRIM(pmpfiuserprofile) AS user_profile_id_text,
        CAST(valid_from_date as datetime),
        DATETIME("9999-12-31T23:59:59") AS valid_to_date,
        pfiworkunit AS workunit_num,
        pmpttaskname AS task_name,
        pmpttasktype AS task_type_num,
        pfiqueueassignment AS queue_assigment_num,
        CAST(actiondate as datetime) AS action_start_date_time,
        actiontaken AS action_taken_text,
        comment AS comment_text,
        authenticatedactor AS authenticated_author_text,
        b.lawson_company_num AS lawson_company_num,
        b.process_level_code AS process_level_code,
        'Y' AS active_dw_ind,
        'L' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND)  AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pfimetrics AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
        AND UPPER(b.source_system_code) = 'L'
        AND b.valid_to_date = DATETIME '9999-12-31 23:59:59';

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_workunit_metric_wrk (workunit_sid, activity_seq_num, user_profile_id_text, valid_from_date, valid_to_date, workunit_num, task_name, task_type_num, queue_assigment_num, action_start_date_time, action_taken_text, comment_text, authenticated_author_text, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid,
        CAST(pfiactivity as INT64) AS activity_seq_num,
        --pfiuserprofile 
		    TRIM(pfimetrics_pfiuserprofile) AS user_profile_id_text,
        CAST(valid_from_date as datetime),
        DATETIME("9999-12-31T23:59:59") AS valid_to_date,
        CAST(pfiworkunit as NUMERIC) AS workunit_num,
        --pfitaskname 
		    pfimetrics_pfitask_taskname AS task_name,
        --pfitasktype 
		    CAST(pfimetrics_pfitask_tasktype  as int64) AS task_type_num,
        CAST(pfiqueueassignment as INT64) AS queue_assigment_num,
        --CAST(a.actiondate as TIMESTAMP) + (a.actiondate - TIME '00:00:00') AS action_start_date_time,
		    CAST(a.actiondate as DATETIME)  AS action_start_date_time,
        actiontaken AS action_taken_text,
        comment AS comment_text,
        authenticatedactor AS authenticated_author_text,
        b.lawson_company_num,
        b.process_level_code,
        'Y' AS active_dw_ind,
        'B' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_pfimetrics_stg AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b 
        ON a.pfiworkunit = b.workunit_num 
		    AND UPPER(b.source_system_code) = 'B'
        AND b.valid_to_date = DATETIME '9999-12-31 23:59:59'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18;
END ;