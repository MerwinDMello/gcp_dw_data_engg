BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);
BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.submission_tracking_status AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND FROM {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_wrk AS wrk WHERE wrk.submission_tracking_sid = tgt.submission_tracking_sid
   AND (coalesce(cast(wrk.submission_status_id as int64), 0 ) <> coalesce(tgt.submission_status_id, 0)
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(wrk.source_system_code), 'X'))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");


  INSERT INTO {{ params.param_hr_core_dataset_name }}.submission_tracking_status (submission_tracking_sid, valid_from_date, valid_to_date, submission_status_id, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.submission_tracking_sid,
        current_dt as Valid_From_Date,
        -- wrk.file_date AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        cast(wrk.submission_status_id as int64),
        wrk.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_wrk AS wrk
      WHERE (coalesce(cast(wrk.submission_tracking_sid as int64), 0), coalesce(cast(wrk.submission_status_id as int64), 0)) NOT IN(
        SELECT AS STRUCT
            coalesce(tgt.submission_tracking_sid, 0),
            coalesce(tgt.submission_status_id, 0)
          FROM
            {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status AS tgt
          WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      )
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_tracking_sid,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.submission_tracking_status
        group by submission_tracking_sid,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.submission_tracking_status');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;