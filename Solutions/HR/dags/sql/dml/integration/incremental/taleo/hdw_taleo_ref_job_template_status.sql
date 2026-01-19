BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_job_template_status AS tgt USING (
    SELECT
        number,
        description,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_jobtemplatestate
  ) AS src
  ON tgt.job_template_status_id = src.number
     WHEN MATCHED THEN UPDATE SET job_template_status_desc = src.description, source_system_code = src.source_system_code, dw_last_update_date_time = timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (job_template_status_id, job_template_status_desc, source_system_code, dw_last_update_date_time) VALUES (src.number, src.description, src.source_system_code, timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND))
  ;
    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT job_template_status_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_job_template_status
      GROUP BY job_template_status_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:ref_job_template_status ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;