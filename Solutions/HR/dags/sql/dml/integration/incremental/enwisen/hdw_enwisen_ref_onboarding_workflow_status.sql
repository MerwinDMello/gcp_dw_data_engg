BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow_status AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.workflow_status_id IS NOT NULL THEN tgt_0.workflow_status_id
          ELSE (
            SELECT
                coalesce(max(ref_onboarding_workflow_status.workflow_status_id), CAST(0 as int64))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow_status
          ) + CAST(row_number() OVER (ORDER BY workflow_status_id, stg.workflow_status_text) as int64)
        END AS workflow_status_id,
        stg.workflow_status_text AS workflow_status_text,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(workflowstatus) AS workflow_status_text,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(workflowstatus) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow_status AS tgt_0 ON tgt_0.workflow_status_text = stg.workflow_status_text
         AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
  ) AS src
  ON tgt.workflow_status_id = src.workflow_status_id
   AND upper(tgt.source_system_code) = upper(src.source_system_code)
     WHEN MATCHED THEN UPDATE SET dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (workflow_status_id, workflow_status_text, source_system_code, dw_last_update_date_time) VALUES (src.workflow_status_id, src.workflow_status_text, src.source_system_code, src.dw_last_update_date_time);

    /* Test Unique Primary Index constraint set in Teradata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Workflow_Status_ID
        from {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow_status
        group by Workflow_Status_ID
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: ' , '{{ params.param_hr_core_dataset_name }}' , '.ref_onboarding_workflow_status');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;	 
  