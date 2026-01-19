/*  Truncate Worktable Table     */
BEGIN
  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.submission_state_wrk;


/*  Load Work Table with working Data */


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_state_wrk (file_date, submission_sid, submission_state_id, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date AS file_date,
        can.submission_sid AS submission_sid,
        CAST(stg.applicationstate_number as INT64) AS submission_state_id,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_application AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS can ON stg.number = can.submission_num
         AND can.valid_to_date = DATETIME("9999-12-31 23:59:59")
      GROUP BY 1, 2, 3
  ;
END;