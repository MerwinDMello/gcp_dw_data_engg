BEGIN
DECLARE current_ts datetime;
DECLARE dup_count int64;

SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.hr_workunit_metric AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, 
  dw_last_update_date_time = stg.dw_last_update_date_time, active_dw_ind = 'N' FROM {{ params.param_hr_stage_dataset_name }}.hr_workunit_metric_wrk AS stg
  WHERE tgt.workunit_sid = stg.workunit_sid
   AND tgt.activity_seq_num = stg.activity_seq_num
   AND tgt.user_profile_id_text = stg.user_profile_id_text
   AND (coalesce(tgt.workunit_num, -999) <> coalesce(stg.workunit_num, -999)
   OR coalesce(trim(tgt.task_name), 'X') <> coalesce(trim(stg.task_name), 'X')
   OR coalesce(tgt.queue_assigment_num, -999) <> coalesce(stg.queue_assigment_num, -999)
   OR coalesce(tgt.task_type_num, -999) <> coalesce(stg.task_type_num, -999)
   OR coalesce(tgt.action_start_date_time, DATE '1901-01-01') <> coalesce(stg.action_start_date_time, DATE '1901-01-01')
   OR coalesce(trim(tgt.action_taken_text), 'X') <> coalesce(trim(stg.action_taken_text), 'X')
   OR coalesce(tgt.comment_text, '-999') <> coalesce(stg.comment_text, '-999')
   OR coalesce(trim(tgt.authenticated_author_text), 'X') <> coalesce(trim(stg.authenticated_author_text), 'X')
   OR coalesce(tgt.lawson_company_num, -999) <> coalesce(stg.lawson_company_num, -999)
   OR coalesce(trim(tgt.process_level_code), 'X') <> coalesce(trim(stg.process_level_code), 'X')
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X')
   OR coalesce(trim(tgt.active_dw_ind), 'X') <> coalesce(trim(stg.active_dw_ind), 'X'))
   AND tgt.valid_to_date = DATETIME '9999-12-31 23:59:59';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.hr_workunit_metric (workunit_sid, activity_seq_num, user_profile_id_text, valid_from_date, valid_to_date, workunit_num, task_name, task_type_num, queue_assigment_num, action_start_date_time, action_taken_text, comment_text, authenticated_author_text, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.workunit_sid,
        stg.activity_seq_num,
        stg.user_profile_id_text,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.workunit_num,
        stg.task_name,
        stg.task_type_num,
        stg.queue_assigment_num,
        stg.action_start_date_time,
        stg.action_taken_text,
        stg.comment_text,
        stg.authenticated_author_text,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.active_dw_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hr_workunit_metric_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_metric AS tgt ON stg.workunit_sid = tgt.workunit_sid
         AND tgt.activity_seq_num = stg.activity_seq_num
         AND tgt.user_profile_id_text = stg.user_profile_id_text
         AND coalesce(tgt.workunit_num, -999) = coalesce(stg.workunit_num, -999)
         AND coalesce(trim(tgt.task_name), 'X') = coalesce(trim(stg.task_name), 'X')
         AND coalesce(tgt.task_type_num, -999) = coalesce(stg.task_type_num, -999)
         AND coalesce(tgt.queue_assigment_num, -999) = coalesce(stg.queue_assigment_num, -999)
         AND coalesce(tgt.action_start_date_time, DATE '1901-01-01') = coalesce(stg.action_start_date_time, DATE '1901-01-01')
         AND coalesce(trim(tgt.action_taken_text), 'X') = coalesce(trim(stg.action_taken_text), 'X')
         AND coalesce(tgt.comment_text, '-999') = coalesce(stg.comment_text, '-999')
         AND coalesce(trim(tgt.authenticated_author_text), 'X') = coalesce(trim(stg.authenticated_author_text), 'X')
         AND coalesce(tgt.lawson_company_num, -999) = coalesce(stg.lawson_company_num, -999)
         AND coalesce(trim(tgt.process_level_code), 'X') = coalesce(trim(stg.process_level_code), 'X')
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND coalesce(trim(tgt.active_dw_ind), 'X') = coalesce(trim(stg.active_dw_ind), 'X')
         AND tgt.valid_to_date = DATETIME '9999-12-31 23:59:59'
      WHERE tgt.workunit_sid IS NULL;

SET
  dup_count = (SELECT COUNT(*) FROM (
                                      SELECT workunit_sid, activity_seq_num, user_profile_id_text, valid_from_date
                                      FROM
                                      {{ params.param_hr_core_dataset_name }}.hr_workunit_metric
                                      GROUP BY workunit_sid, activity_seq_num, user_profile_id_text, valid_from_date
                                      HAVING COUNT(*) > 1 
                                    ) 
              );
IF
  dup_count <> 0 THEN
  ROLLBACK TRANSACTION; raise
  USING
    message = CONCAT('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.hr_workunit_metric');
ELSE
  COMMIT TRANSACTION;
END IF;

END ;