BEGIN
DECLARE current_dt DATETIME;
DECLARE DUP_COUNT INT64;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);



begin transaction;
  UPDATE {{ params.param_hr_core_dataset_name }}.submission_tracking AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(current_datetime(), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk AS wrk WHERE wrk.submission_tracking_sid = tgt.submission_tracking_sid
   AND (coalesce(trim(cast(wrk.candidate_profile_sid as string)), CAST(0 as STRING)) <> coalesce(trim(cast(tgt.candidate_profile_sid as string)), CAST(0 as STRING))
   OR coalesce(trim(cast(wrk.submission_tracking_num as string)), CAST(0 as STRING)) <> coalesce(trim(cast(tgt.submission_tracking_num as string)), CAST(0 as STRING))
   OR coalesce(wrk.creation_date_time, DATE '1900-01-01') <> coalesce(tgt.creation_date_time, DATE '1900-01-01')
   OR coalesce(wrk.event_date_time, DATE '1900-01-01') <> coalesce(tgt.event_date_time, DATE '1900-01-01')
   OR coalesce(trim(wrk.event_detail_text), 'XXX') <> coalesce(trim(tgt.event_detail_text), 'XXX')
   OR coalesce(trim(cast(wrk.submission_event_id as string)), CAST(0 as STRING)) <> coalesce(trim(cast(tgt.submission_event_id as string)), CAST(0 as STRING))
   OR coalesce(trim(cast(wrk.tracking_user_sid as string)), CAST(0 as STRING)) <> coalesce(trim(cast(tgt.tracking_user_sid as string)), CAST(0 as STRING))
   OR coalesce(trim(cast(wrk.tracking_step_id as string)), CAST(0 as STRING)) <> coalesce(trim(cast(tgt.tracking_step_id as string)), CAST(0 as STRING))
   OR coalesce(trim(cast(wrk.tracking_workflow_id as string)), CAST(0 as STRING)) <> coalesce(trim(cast(tgt.tracking_workflow_id as string)), CAST(0 as STRING))
   OR coalesce(trim(wrk.step_reverted_ind), 'XXX') <> coalesce(trim(tgt.step_reverted_ind), 'XXX')
   OR coalesce(trim(wrk.sub_status_desc), 'XXX') <> coalesce(trim(tgt.sub_status_desc), 'XXX')
    --TDGCP-2862
   OR coalesce(trim(wrk.moved_by_text),'XXX') <> coalesce(trim(tgt.moved_by_text),'XXX')
   --TDGCP-2862
   OR coalesce(trim(wrk.source_system_code), 'X') <> coalesce(trim(tgt.source_system_code), 'X'))
   AND tgt.valid_to_date = datetime("9999-12-31 23:59:59");

  INSERT INTO {{ params.param_hr_core_dataset_name }}.submission_tracking (submission_tracking_sid, valid_from_date, candidate_profile_sid, submission_tracking_num, creation_date_time, event_date_time, event_detail_text, submission_event_id, tracking_user_sid, tracking_step_id, tracking_workflow_id, sub_status_desc, 
  --TDGCP-2862
  moved_by_text,
  --TDGCP-2862  
  step_reverted_ind, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        -- changed from Submission_Profile_SID
        wrk.submission_tracking_sid AS submission_tracking_sid,
        current_dt as Valid_From_Date,
        -- wrk.file_date AS valid_from_date,
        wrk.candidate_profile_sid AS candidate_profile_sid,
        -- 20 as Submission_Profile_SID AS ,
        wrk.submission_tracking_num AS submission_tracking_num,
        wrk.creation_date_time AS creation_date_time,
        wrk.event_date_time AS event_date_time,
        wrk.event_detail_text AS event_detail_text,
        wrk.submission_event_id AS submission_event_id,
        wrk.tracking_user_sid AS tracking_user_sid,
        -- 15 as Tracking_User_SID AS,
        wrk.tracking_step_id AS tracking_step_id,
        wrk.tracking_workflow_id AS tracking_workflow_id,
        wrk.sub_status_desc AS sub_status_desc,
		--TDGCP-2862
        wrk.moved_by_text,
        --TDGCP-2862
        wrk.step_reverted_ind AS step_reverted_ind,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        wrk.source_system_code AS source_system_code,
        DATETIME_TRUNC(current_datetime(), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk AS wrk
      WHERE (coalesce(trim(cast(wrk.candidate_profile_sid as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.submission_tracking_num as string)), CAST(0 as STRING)), coalesce(wrk.creation_date_time, DATETIME '1900-01-01'), coalesce(wrk.event_date_time, DATE '1900-01-01'), coalesce(trim(wrk.event_detail_text), 'XXX'), coalesce(trim(cast(wrk.submission_event_id as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.tracking_user_sid as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.tracking_step_id as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.tracking_workflow_id as string)), CAST(0 as STRING)), coalesce(trim(wrk.step_reverted_ind), 'XXX'), coalesce(trim(wrk.sub_status_desc), 'XXX'), 
	  --TDGCP-2862
	  coalesce(trim(wrk.moved_by_text),'XXX'),
	  --TDGCP-2862
	  coalesce(trim(wrk.source_system_code), 'X')) NOT IN(
        SELECT AS STRUCT
            coalesce(trim(cast(tgt.candidate_profile_sid as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.submission_tracking_num as string)), CAST(0 as STRING)),
            coalesce(tgt.creation_date_time, DATE '1900-01-01'),
            coalesce(tgt.event_date_time, DATE '1900-01-01'),
            coalesce(trim(tgt.event_detail_text), 'XXX'),
            coalesce(trim(cast(tgt.submission_event_id as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.tracking_user_sid as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.tracking_step_id as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.tracking_workflow_id as string)), CAST(0 as STRING)),
            coalesce(trim(tgt.step_reverted_ind), 'XXX'),
            coalesce(trim(tgt.sub_status_desc), 'XXX'),
			--TDGCP-2862
			coalesce(trim(tgt.moved_by_text),'XXX'),
			--TDGCP-2862
            coalesce(trim(tgt.source_system_code), 'X')
          FROM
            {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS tgt
          WHERE (tgt.valid_to_date) = datetime("9999-12-31 23:59:59")
      )
  ;

  SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Submission_Tracking_SID ,
Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.submission_tracking
    GROUP BY
      Submission_Tracking_SID ,
Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.submission_tracking');
  ELSE
COMMIT TRANSACTION;
END IF
;end