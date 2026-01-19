/*  LOAD WORK TABLE WITH WORKING DATA */
BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_submission_status AS tgt USING (
    SELECT
        CAST(number as INT64) AS submission_status_id,
        available AS active_sw,
        applicationstate_number AS submission_state_id,
        trim(mnemonic) AS submission_status_code,
        trim(name) AS submission_status_name,
        trim(description) AS submission_status_desc,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_cswstatus
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ) AS stg
  ON tgt.submission_status_id = stg.submission_status_id
     WHEN MATCHED THEN UPDATE SET active_sw = CAST(stg.active_sw as INT64), submission_state_id = CAST(stg.submission_state_id as INT64), submission_status_code = stg.submission_status_code, submission_status_name = stg.submission_status_name, submission_status_desc = stg.submission_status_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (submission_status_id, active_sw, submission_state_id, submission_status_code, submission_status_name, submission_status_desc, source_system_code, dw_last_update_date_time) VALUES (stg.submission_status_id, CAST(stg.active_sw as INT64), CAST(stg.submission_state_id as INT64), stg.submission_status_code, stg.submission_status_name, stg.submission_status_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;

--  ATS code to generate the ID HDM-1448

  
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_submission_status AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.submission_status_id IS NOT NULL THEN tgt_0.submission_status_id
          ELSE (
            SELECT
                coalesce(max(submission_status_id), CAST(1000000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_submission_status
              WHERE upper(stg.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY stg.submission_status_code) as BIGNUMERIC)
        END AS submission_status_id,
        stg.active_sw,
        stg.submission_state_id,
        stg.submission_status_code,
        stg.submission_status_name,
        stg.submission_status_desc,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              ws.active AS active_sw,
              '0' AS submission_state_id,
              trim('1/' || ws.atsworkflow || '/' || ws.atsworkflowstep) AS submission_status_code,
              trim(ws.atsworkflowstep) AS submission_status_name,
              trim(ws.description) AS submission_status_desc,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS ws
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS tgt_0 ON trim(tgt_0.submission_status_code) = trim(stg.submission_status_code)
         AND upper(tgt_0.source_system_code) = 'B'
  ) AS src
  ON tgt.submission_status_id = src.submission_status_id
     WHEN MATCHED THEN UPDATE SET active_sw = CAST(src.active_sw as INT64), submission_state_id = CAST(src.submission_state_id as INT64), submission_status_code = src.submission_status_code, submission_status_name = src.submission_status_name, submission_status_desc = src.submission_status_desc, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (submission_status_id, active_sw, submission_state_id, submission_status_code, submission_status_name, submission_status_desc, source_system_code, dw_last_update_date_time) VALUES (CAST(src.submission_status_id as INT64), CAST(src.active_sw as INT64), CAST(src.submission_state_id as INT64), src.submission_status_code, src.submission_status_name, src.submission_status_desc, src.source_system_code, src.dw_last_update_date_time)
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Submission_Status_Id
        from {{ params.param_hr_core_dataset_name }}.ref_submission_status
        group by Submission_Status_Id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_submission_status');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;