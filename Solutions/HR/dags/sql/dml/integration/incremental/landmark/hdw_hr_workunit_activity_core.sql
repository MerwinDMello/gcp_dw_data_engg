BEGIN

DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

  UPDATE {{ params.param_hr_core_dataset_name }}.hr_workunit_activity AS tgt 
    SET valid_to_date =  current_ts - INTERVAL 1 SECOND, 
  dw_last_update_date_time = stg.dw_last_update_date_time, active_dw_ind = 'N' FROM {{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk AS stg 
  WHERE tgt.workunit_sid = stg.workunit_sid
   AND tgt.activity_seq_num = stg.activity_seq_num
   AND (coalesce(tgt.workunit_num, -999) <> coalesce(stg.workunit_num, -999)
   OR coalesce(tgt.start_date_time, DATE '1901-01-01') <> coalesce(stg.start_date_time, DATE '1901-01-01')
   OR coalesce(tgt.end_date_time, DATE '1901-01-01') <> coalesce(stg.end_date_time, DATE '1901-01-01')
   OR coalesce(trim(tgt.activity_name), 'X') <> coalesce(trim(stg.activity_name), 'X')
   OR coalesce(trim(tgt.activity_type_text), 'X') <> coalesce(trim(stg.activity_type_text), 'X')
   OR coalesce(tgt.action_taken_text, '-999') <> coalesce(stg.action_taken_text, '-999')
   OR coalesce(trim(tgt.node_caption_text), 'X') <> coalesce(trim(stg.node_caption_text), 'X')
   OR coalesce(trim(tgt.user_profile_id_text), 'X') <> coalesce(trim(stg.user_profile_id_text), 'X')
   OR coalesce(trim(tgt.authenticated_author_text), 'X') <> coalesce(trim(stg.authenticated_author_text), 'X')
   OR coalesce(trim(tgt.task_name), 'X') <> coalesce(trim(stg.task_name), 'X')
   OR coalesce(tgt.task_type_num, -999) <> coalesce(stg.task_type_num, -999)
   OR coalesce(tgt.lawson_company_num, -999) <> coalesce(stg.lawson_company_num, -999)
   OR coalesce(trim(tgt.process_level_code), 'X') <> coalesce(trim(stg.process_level_code), 'X')
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X')
   OR coalesce(trim(tgt.active_dw_ind), 'X') <> coalesce(trim(stg.active_dw_ind), 'X'))
   AND tgt.valid_to_date = datetime '9999-12-31 23:59:59';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.hr_workunit_activity (workunit_sid, activity_seq_num, valid_from_date, valid_to_date, workunit_num, start_date_time, end_date_time, activity_name, activity_type_text, action_taken_text, node_caption_text, user_profile_id_text, authenticated_author_text, task_name, task_type_num, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.workunit_sid,
        stg.activity_seq_num,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.workunit_num,
        stg.start_date_time,
        stg.end_date_time,
        stg.activity_name,
        stg.activity_type_text,
        stg.action_taken_text,
        stg.node_caption_text,
        stg.user_profile_id_text,
        stg.authenticated_author_text,
        stg.task_name,
        stg.task_type_num,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.active_dw_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_activity AS tgt ON stg.workunit_sid = tgt.workunit_sid
         AND tgt.activity_seq_num = stg.activity_seq_num
         AND coalesce(tgt.workunit_num, -999) = coalesce(stg.workunit_num, -999)
         AND coalesce(tgt.start_date_time, DATE '1901-01-01') = coalesce(stg.start_date_time, DATE '1901-01-01')
         AND coalesce(tgt.end_date_time, DATE '1901-01-01') = coalesce(stg.end_date_time, DATE '1901-01-01')
         AND coalesce(trim(tgt.activity_name), 'X') = coalesce(trim(stg.activity_name), 'X')
         AND coalesce(trim(tgt.activity_type_text), 'X') = coalesce(trim(stg.activity_type_text), 'X')
         AND coalesce(trim(tgt.action_taken_text), 'X') = coalesce(trim(stg.action_taken_text), 'X')
         AND coalesce(trim(tgt.node_caption_text), 'X') = coalesce(trim(stg.node_caption_text), 'X')
         AND coalesce(trim(tgt.user_profile_id_text), 'X') = coalesce(trim(stg.user_profile_id_text), 'X')
         AND coalesce(trim(tgt.authenticated_author_text), 'X') = coalesce(trim(stg.authenticated_author_text), 'X')
         AND coalesce(trim(tgt.task_name), 'X') = coalesce(trim(stg.task_name), 'X')
         AND coalesce(tgt.task_type_num, -999) = coalesce(stg.task_type_num, -999)
         AND coalesce(tgt.lawson_company_num, -999) = coalesce(stg.lawson_company_num, -999)
         AND coalesce(trim(tgt.process_level_code), 'X') = coalesce(trim(stg.process_level_code), 'X')
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND coalesce(trim(tgt.active_dw_ind), 'X') = coalesce(trim(stg.active_dw_ind), 'X')
         AND tgt.valid_to_date=datetime '9999-12-31 23:59:59'
      WHERE tgt.workunit_sid IS NULL
  ;
END;