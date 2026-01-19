BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);
BEGIN TRANSACTION;


UPDATE {{ params.param_hr_core_dataset_name }}.junc_submission_tracking_motive AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) FROM {{ params.param_hr_stage_dataset_name }}.junc_submission_tracking_motive_wrk AS wrk WHERE wrk.submission_tracking_sid = tgt.submission_tracking_sid
 AND wrk.tracking_motive_id <> tgt.tracking_motive_id
 AND (tgt.valid_to_date) = DATETIME('9999-12-31 23:59:59'); 


  INSERT INTO {{ params.param_hr_core_dataset_name }}.junc_submission_tracking_motive (submission_tracking_sid, tracking_motive_id, valid_from_date, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.submission_tracking_sid,
        wrk.tracking_motive_id,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        wrk.source_system_code AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
       {{ params.param_hr_stage_dataset_name }}.junc_submission_tracking_motive_wrk AS wrk
      WHERE (trim(CAST(coalesce(wrk.submission_tracking_sid, 0) as STRING)), trim(CAST(coalesce(wrk.tracking_motive_id, 0) as STRING)), wrk.source_system_code) NOT IN(
        SELECT AS STRUCT
            trim(CAST(coalesce(tgt.submission_tracking_sid, 0) as STRING)),
            trim(CAST(coalesce(tgt.tracking_motive_id, 0) as STRING)),
            tgt.source_system_code
          FROM
            {{ params.param_hr_base_views_dataset_name }}.junc_submission_tracking_motive AS tgt
          WHERE tgt.valid_to_date =DATETIME("9999-12-31 23:59:59")
      )
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_tracking_sid,tracking_motive_id,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.junc_submission_tracking_motive
        group by submission_tracking_sid,tracking_motive_id,valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.junc_submission_tracking_motive');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;