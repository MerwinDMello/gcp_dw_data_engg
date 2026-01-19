
/*  Load Work Table with working Data */
BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;
  
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_submission_state AS tgt USING (
    SELECT DISTINCT
        CASE
           trim(app_med.number)
          WHEN '' THEN 0
          ELSE CAST(trim(app_med.number) as INT64)
        END AS submission_state_id,
        trim(app_med.description) AS submission_state_desc,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_applicationstate AS app_med
      WHERE number IS NOT NULL
    UNION ALL
    SELECT
        CAST(CASE
          WHEN tgt_0.submission_state_id IS NOT NULL THEN tgt_0.submission_state_id
          ELSE (
            SELECT
                coalesce(max(submission_state_id), CAST(1000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_submission_state
              WHERE upper(stgg.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY tgt_0.submission_state_id, stgg.submission_state_desc) as BIGNUMERIC)
        END as INT64) AS submission_state_id,
        stgg.submission_state_desc,
        stgg.source_system_code,
        stgg.dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(bct.candidateapplicationstatus) AS submission_state_desc,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS bct
            WHERE coalesce(trim(bct.candidateapplicationstatus), '') <> ''
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_state AS tgt_0 ON trim(tgt_0.submission_state_desc) = trim(stgg.submission_state_desc)
         AND upper(tgt_0.source_system_code) = 'B'
  ) AS stg
  ON tgt.submission_state_id = stg.submission_state_id
   AND upper(tgt.source_system_code) = upper(stg.source_system_code)
     WHEN MATCHED THEN UPDATE SET submission_state_desc = stg.submission_state_desc, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (submission_state_id, submission_state_desc, source_system_code, dw_last_update_date_time) VALUES (stg.submission_state_id, stg.submission_state_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;
  SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_state_id
        from {{ params.param_hr_core_dataset_name }}.ref_submission_state
        group by submission_state_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_submission_state');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
