BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  BEGIN TRANSACTION;

  /* Retire the records those are changed */ /* Begin Transaction Block Starts Here */
UPDATE
  {{ params.param_hr_core_dataset_name }}.recruitment_job AS tgt
SET
  valid_to_date =  current_dt- INTERVAL 1 SECOND,
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk AS wrk
WHERE
  wrk.recruitment_job_sid = tgt.recruitment_job_sid
    AND (
    COALESCE(wrk.recruitment_job_num, 123456) <> COALESCE(tgt.recruitment_job_num, 123456 )
    OR UPPER(COALESCE(TRIM(wrk.job_title_name), 'XXX')) <> UPPER(COALESCE(TRIM(tgt.job_title_name), 'XXX'))
    OR COALESCE(TRIM(wrk.job_grade_code), 'XXX') <> COALESCE(TRIM(tgt.job_grade_code), 'XXX')
    OR COALESCE(wrk.job_schedule_id, 123456) <> COALESCE(tgt.job_schedule_id,123456)
    OR COALESCE(wrk.overtime_status_id,123456) <> COALESCE(tgt.overtime_status_id,123456)
    OR COALESCE(wrk.primary_facility_location_num,123456) <> COALESCE(tgt.primary_facility_location_num,123456)
    OR COALESCE(wrk.recruiter_user_sid,123456) <> COALESCE(tgt.recruiter_user_sid,123456)
    OR COALESCE(wrk.recruitment_job_parameter_sid,123456) <> COALESCE(tgt.recruitment_job_parameter_sid,123456)
    OR COALESCE(wrk.recruitment_position_sid,123456) <> COALESCE(tgt.recruitment_position_sid,123456)
    OR COALESCE(wrk.fte_pct,123456) <> COALESCE(tgt.fte_pct,123456)
    OR COALESCE(TRIM(wrk.source_system_code), 'XXX') <> COALESCE(TRIM(tgt.source_system_code), 'XXX'))
  AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.recruitment_job (recruitment_job_sid,
    valid_from_date,
    valid_to_date,
    recruitment_job_num,
    job_title_name,
    job_grade_code,
    job_schedule_id,
    overtime_status_id,
    primary_facility_location_num,
    recruiter_user_sid,
    recruitment_job_parameter_sid,
    recruitment_position_sid,
    fte_pct,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.recruitment_job_sid,
  current_dt AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.recruitment_job_num,
  wrk.job_title_name,
  wrk.job_grade_code,
  wrk.job_schedule_id,
  wrk.overtime_status_id,
  wrk.primary_facility_location_num,
  wrk.recruiter_user_sid,
  wrk.recruitment_job_parameter_sid,
  wrk.recruitment_position_sid,
  wrk.fte_pct,
  wrk.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk AS wrk
WHERE
    
(COALESCE(wrk.recruitment_job_sid,123456),
    COALESCE(wrk.recruitment_job_num,123456),
    UPPER(COALESCE(TRIM(wrk.job_title_name), 'XXX')),
    COALESCE(TRIM(wrk.job_grade_code), 'XXX'),
    COALESCE(wrk.job_schedule_id,123456),
    COALESCE(wrk.overtime_status_id,123456),
    COALESCE(wrk.primary_facility_location_num,123456),
    COALESCE(wrk.recruiter_user_sid,123456),
    COALESCE(wrk.recruitment_job_parameter_sid,123456),
    COALESCE(wrk.recruitment_position_sid,123456),
    COALESCE(wrk.fte_pct,123456),
    COALESCE(TRIM(wrk.source_system_code), 'XXX')) NOT IN(
  SELECT
    AS STRUCT COALESCE(tgt.recruitment_job_sid,123456),
    COALESCE(tgt.recruitment_job_num,123456),
    UPPER(COALESCE(TRIM(tgt.job_title_name), 'XXX')),
    COALESCE(TRIM(tgt.job_grade_code), 'XXX'),
    COALESCE(tgt.job_schedule_id,123456),
    COALESCE(tgt.overtime_status_id,123456),
    COALESCE(tgt.primary_facility_location_num,123456),
    COALESCE(tgt.recruiter_user_sid,123456),
    COALESCE(tgt.recruitment_job_parameter_sid,123456),
    COALESCE(tgt.recruitment_position_sid,123456),
    COALESCE(tgt.fte_pct,123456),
    COALESCE(TRIM(tgt.source_system_code), 'XXX')
  FROM
    {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS tgt
  WHERE
    (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59") ) ;

/* Test Unique Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
        recruitment_job_sid,valid_from_date            
        from {{ params.param_hr_core_dataset_name }}.recruitment_job
        group by 
        recruitment_job_sid,valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:{{ params.param_hr_core_dataset_name }}.recruitment_job' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;    