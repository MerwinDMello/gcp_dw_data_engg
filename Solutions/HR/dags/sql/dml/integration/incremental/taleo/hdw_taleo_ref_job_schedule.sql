BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_job_schedule AS tgt USING (
    SELECT
        number,
        active,
        code,
        description,
        sequence,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_jobschedule
  ) AS src
  ON tgt.job_schedule_id = src.number
     WHEN MATCHED THEN UPDATE SET active_sw = src.active, job_schedule_code = src.code, job_schedule_desc = src.description, job_schedule_seq_num = src.sequence, source_system_code = src.source_system_code, dw_last_update_date_time = timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (job_schedule_id, active_sw, job_schedule_code, job_schedule_desc, job_schedule_seq_num, source_system_code, dw_last_update_date_time) VALUES (src.number, src.active, src.code, src.description, src.sequence, src.source_system_code, timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND))
  ;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_job_schedule AS tgt USING (
    SELECT DISTINCT
        CASE
          WHEN job_schedule_id IS NOT NULL THEN job_schedule_id
          ELSE (
            SELECT
                coalesce(max(job_schedule_id), CAST(10000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule
              WHERE upper(ref_job_schedule.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY stg.worktype) as BIGNUMERIC)
        END AS job_schedule_id,
        stg.active AS active_sw,
        stg.worktype AS job_schedule_code,
        stg.description AS job_schedule_desc,
        CAST(NULL as INT64) AS job_schedule_seq_num,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_hcm_worktype_stg AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule AS tgt_0 ON trim(tgt_0.job_schedule_code) = trim(stg.worktype)
         AND upper(tgt_0.source_system_code) = 'B'
      WHERE upper(stg.worktype) <> ''
  ) AS src
  ON tgt.job_schedule_id = src.job_schedule_id
     WHEN MATCHED THEN UPDATE SET active_sw = CAST(src.active_sw as INT64), job_schedule_desc = src.job_schedule_desc, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (job_schedule_id, active_sw, job_schedule_code, job_schedule_desc, job_schedule_seq_num, source_system_code, dw_last_update_date_time) VALUES (CAST(src.job_schedule_id AS INT64), CAST(src.active_sw as INT64), src.job_schedule_code, src.job_schedule_desc, src.job_schedule_seq_num, src.source_system_code, src.dw_last_update_date_time)
  ;
    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Job_Schedule_Id
        from {{ params.param_hr_core_dataset_name }}.ref_job_schedule
        group by Job_Schedule_Id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_job_schedule');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
