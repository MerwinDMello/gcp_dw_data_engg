BEGIN
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk (workunit_sid, activity_seq_num, valid_from_date, valid_to_date, workunit_num, start_date_time, end_date_time, activity_name, activity_type_text, action_taken_text, node_caption_text, user_profile_id_text, authenticated_author_text, task_name, task_type_num, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid AS workunit_sid,
        pfiactivity AS activity_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31T23:59:59") AS valid_to_date,
        pfiworkunit AS workunit_num,
        --startdate AS start_date_time,
        --enddate AS end_date_time,
		CAST(startdate as datetime)  AS start_date_time,
        CAST(enddate as datetime)  AS end_date_time,
        trim(activityname) AS activity_name,
        trim(activitytype) AS activity_type_text,
        trim(actiontaken) AS action_taken_text,
        trim(nodecaption) AS node_caption_text,
        trim(pfiuserprofile) AS user_profile_id_text,
        trim(authenticatedactor) AS authenticated_author_text,
        trim(pttaskname) AS task_name,
        pttasktype AS task_type_num,
        b.lawson_company_num AS lawson_company_num,
        b.process_level_code AS process_level_code,
        'Y' AS active_dw_ind,
        'L' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pfimetricssummary AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'L'
         AND b.valid_to_date=datetime '9999-12-31 23:59:59';


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk (workunit_sid, activity_seq_num, valid_from_date, valid_to_date, workunit_num, start_date_time, end_date_time, activity_name, activity_type_text, action_taken_text, node_caption_text, user_profile_id_text, authenticated_author_text, task_name, task_type_num, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid AS workunit_sid,
        pfiactivity AS activity_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31T23:59:59") AS valid_to_date,
        pfiworkunit AS workunit_num,
        --CAST(a.startdate as TIMESTAMP) + (a.startdate - TIME '00:00:00') AS start_date_time,
        --CAST(a.enddate as TIMESTAMP) + (a.enddate - TIME '00:00:00') AS end_date_time,
		CAST(a.startdate as datetime)  AS start_date_time,
        CAST(a.enddate as datetime)  AS end_date_time,
        activityname AS activity_name,
        activitytype AS activity_type_text,
        actstatus_state AS action_taken_text,
        cast(null as string) node_caption_text,
        cast(null as string) user_profile_id_text,
        cast(null as string) authenticated_author_text,
        cast(null as string) task_name,
        NULL AS task_type_num,
        b.lawson_company_num AS lawson_company_num,
        b.process_level_code AS process_level_code,
        'Y' AS active_dw_ind,
        'B' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiactivity_stg AS a
        --{{ params.param_hr_stage_dataset_name }}.ats_pfiactivity_stg As a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'B'
         AND b.valid_to_date = datetime '9999-12-31 23:59:59'
      QUALIFY row_number() OVER (PARTITION BY workunit_sid, activity_seq_num ORDER BY end_date_time DESC) = 1  ;
END ;

