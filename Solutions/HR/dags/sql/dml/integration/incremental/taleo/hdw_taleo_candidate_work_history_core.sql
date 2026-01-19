BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

	  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_work_history AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) FROM {{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk AS stg WHERE tgt.candidate_work_history_sid = stg.candidate_work_history_sid
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND (coalesce((tgt.candidate_work_history_num), CAST(-999 as INT64)) <> coalesce((stg.candidate_work_history_num), CAST(-999 as INT64))
   OR coalesce((tgt.candidate_profile_sid), CAST(-999 as INT64)) <> coalesce((stg.candidate_profile_sid), CAST(-999 as INT64))
   OR coalesce((tgt.candidate_sid), CAST(-999 as INT64)) <> coalesce((stg.candidate_sid), CAST(-999 as INT64))
   OR coalesce(tgt.work_start_date, DATE '1901-01-01') <> coalesce(stg.work_start_date, DATE '1901-01-01')
   OR coalesce(tgt.work_end_date, DATE '1901-01-01') <> coalesce(stg.work_end_date, DATE '1901-01-01')
   OR coalesce((tgt.current_employer_sw), CAST(-9 as INT64)) <> coalesce((stg.current_employer_sw), CAST(-9 as INT64))
   OR coalesce((tgt.profile_display_seq_num), CAST(-999 as INT64)) <> coalesce((stg.profile_display_seq_num), CAST(-999 as INT64))
   OR coalesce(trim(tgt.employer_name), 'XX') <> coalesce(trim(stg.employer_name), 'XX')
   OR coalesce(trim(tgt.job_title_name), 'YY') <> coalesce(trim(stg.job_title_name), 'YY'));


  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_work_history (candidate_work_history_sid, valid_from_date, valid_to_date, candidate_work_history_num, candidate_profile_sid, candidate_sid, work_start_date, work_end_date, current_employer_sw, profile_display_seq_num, employer_name, job_title_name, source_system_code, dw_last_update_date_time)
    SELECT
        stg.candidate_work_history_sid,
        -- ,	CURRENT_DATE as Valid_From_Date
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.candidate_work_history_num,
        stg.candidate_profile_sid,
        stg.candidate_sid,
        stg.work_start_date,
        stg.work_end_date,
        stg.current_employer_sw,
        stg.profile_display_seq_num,
        stg.employer_name,
        stg.job_title_name,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_work_history AS tgt ON stg.candidate_work_history_sid = tgt.candidate_work_history_sid
         AND coalesce((tgt.candidate_work_history_num), CAST(-999 as INT64)) = coalesce((stg.candidate_work_history_num), CAST(-999 as INT64))
         AND coalesce((tgt.candidate_profile_sid), CAST(-999 as INT64)) = coalesce((stg.candidate_profile_sid), CAST(-999 as INT64))
         AND coalesce((tgt.candidate_sid), CAST(-999 as INT64)) = coalesce((stg.candidate_sid), CAST(-999 as INT64))
         AND coalesce(tgt.work_start_date, DATE '1901-01-01') = coalesce(stg.work_start_date, DATE '1901-01-01')
         AND coalesce(tgt.work_end_date, DATE '1901-01-01') = coalesce(stg.work_end_date, DATE '1901-01-01')
         AND coalesce((tgt.current_employer_sw), CAST(-9 as INT64)) = coalesce((stg.current_employer_sw), CAST(-9 as INT64))
         AND coalesce((tgt.profile_display_seq_num), CAST(-999 as INT64)) = coalesce((stg.profile_display_seq_num), CAST(-999 as INT64))
         AND coalesce(trim(tgt.employer_name), 'XX') = coalesce(trim(stg.employer_name), 'XX')
         AND coalesce(trim(tgt.job_title_name), 'YY') = coalesce(trim(stg.job_title_name), 'YY')
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.candidate_work_history_sid IS NULL
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
             candidate_work_history_sid ,valid_from_date  
        from {{ params.param_hr_core_dataset_name }}.candidate_work_history
        group by  candidate_work_history_sid ,valid_from_date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_work_history');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;