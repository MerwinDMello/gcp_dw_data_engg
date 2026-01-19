BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
   
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.workflow_id IS NOT NULL THEN tgt_0.workflow_id
          ELSE (
            SELECT
                coalesce(max(ref_onboarding_workflow.workflow_id), CAST(0 as int64))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow
          ) + CAST(row_number() OVER (ORDER BY workflow_id, stg.workflow_name) as int64)
        END AS workflow_id,
        stg.workflow_name AS workflow_name,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(workflowname) AS workflow_name,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(workflowname) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow AS tgt_0 ON tgt_0.workflow_name = stg.workflow_name
         AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
  ) AS src
  ON tgt.workflow_id = src.workflow_id
   AND upper(tgt.source_system_code) = upper(src.source_system_code)
     WHEN MATCHED THEN UPDATE SET dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (workflow_id, workflow_name, source_system_code, dw_last_update_date_time) VALUES (src.workflow_id, src.workflow_name, src.source_system_code, src.dw_last_update_date_time);

    /* Test Unique Primary Index constraint set in Teradata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Workflow_ID
        from {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow
        group by Workflow_ID
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: ' , '{{ params.param_hr_core_dataset_name }}' , '.ref_onboarding_workflow');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;