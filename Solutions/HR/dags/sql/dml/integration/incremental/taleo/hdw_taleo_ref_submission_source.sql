BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_submission_source AS tgt USING (
    SELECT DISTINCT
        trim(specificsource) AS submission_source_code,
        trim(description) AS submission_source_desc,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_hcm_specificsource_stg
  ) AS stg
  ON tgt.submission_source_code = stg.submission_source_code
   AND upper(tgt.source_system_code) = upper(stg.source_system_code)
     WHEN MATCHED THEN UPDATE SET submission_source_desc = stg.submission_source_desc, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (submission_source_code, submission_source_desc, source_system_code, dw_last_update_date_time) VALUES (stg.submission_source_code, stg.submission_source_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;


    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Submission_Source_Code 
        from {{ params.param_hr_core_dataset_name }}.ref_submission_source
        group by Submission_Source_Code 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_submission_source');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
   
