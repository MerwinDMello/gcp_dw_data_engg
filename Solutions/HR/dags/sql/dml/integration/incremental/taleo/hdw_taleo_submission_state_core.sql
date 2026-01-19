BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);

BEGIN TRANSACTION;
  
  UPDATE {{ params.param_hr_core_dataset_name }}.submission_state AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.submission_state_wrk AS wrk WHERE wrk.submission_sid = tgt.submission_sid
   AND coalesce(wrk.submission_state_id, 0 ) <> coalesce(tgt.submission_state_id, 0 )
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");


  INSERT INTO {{ params.param_hr_core_dataset_name }}.submission_state (submission_sid, valid_from_date, valid_to_date, submission_state_id, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.submission_sid,
        current_dt,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        wrk.submission_state_id,
        wrk.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_state_wrk AS wrk
      WHERE (coalesce(wrk.submission_sid, 0 ), coalesce(wrk.submission_state_id, 0 )) NOT IN(
        SELECT AS STRUCT
            coalesce(tgt.submission_sid, 0 ),
            coalesce(tgt.submission_state_id, 0)
          FROM
            {{ params.param_hr_base_views_dataset_name }}.submission_state AS tgt
          WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      )
  ;
       SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_sid,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.submission_state
        group by submission_sid,valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.submission_state');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;