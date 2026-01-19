BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_submission_event_category AS tgt 
  USING (
    SELECT
        number AS submission_event_category_id,
        code,
        description,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidateselectioneventcatg
    UNION ALL
    SELECT
        CASE
          WHEN submission_event_category_id IS NOT NULL THEN submission_event_category_id
          ELSE (
            SELECT
                coalesce(max(submission_event_category_id), CAST(1000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category
              WHERE upper(stg.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY submission_event_category_id, stg.code) as BIGNUMERIC)
        END AS submission_event_category_id,
        stg.code,
        stg.description,
        stg.source_system_code AS source_system_code,
        stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              actionlabel AS code,
              actionlabel AS description,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_businessaction_bct_stg
            WHERE upper(businessview) = 'JOBAPPLICATION'
          UNION ALL
          SELECT DISTINCT
              type_state AS code,
              type_state AS description,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_edw_useraction_stg
            WHERE actionname NOT IN(
              SELECT DISTINCT
                  businessaction
                FROM
                  {{ params.param_hr_stage_dataset_name }}.ats_businessaction_bct_stg
                WHERE upper(businessview) = 'JOBAPPLICATION'
            )
          UNION ALL
          SELECT DISTINCT
              --  Adding new action from ats_cust_v2_jobapplicationworkflowstephistory_stg
              stg_0.action AS code,
              stg_0.action AS description,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobapplicationworkflowstephistory_stg AS stg_0
            WHERE stg_0.action NOT IN(
              SELECT DISTINCT
                  submission_event_category_code
                FROM
                  {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category
                WHERE Upper(Trim(source_system_code)) = 'B'
            )
             AND coalesce(stg_0.action,'')<>''
             AND stg_0.triggeredby NOT LIKE 'Oferta%'
             AND stg_0.triggeredby NOT LIKE 'Offre acceptÃƒÂ©e'
             AND stg_0.triggeredby NOT LIKE 'Retirar'
             AND stg_0.triggeredby NOT LIKE 'Ã¥%'
             AND stg_0.triggeredby NOT LIKE 'Ã%'
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category AS tgt_0 
        ON COALESCE(UPPER(TRIM(tgt_0.submission_event_category_code)),'') = COALESCE(UPPER(TRIM(stg.code)),'')
         AND COALESCE(UPPER(TRIM(tgt_0.source_system_code)),'') = COALESCE(UPPER(TRIM(stg.source_system_code)),'')
  ) AS src
  ON tgt.submission_event_category_id = src.submission_event_category_id
   AND tgt.source_system_code = src.source_system_code
     WHEN MATCHED THEN UPDATE SET submission_event_category_code = src.code, submission_event_category_desc = src.description, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (submission_event_category_id, submission_event_category_code, submission_event_category_desc, source_system_code, dw_last_update_date_time) 
     VALUES (CAST(src.submission_event_category_id as INT64), src.code, src.description, src.source_system_code, DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND))
  ;
      SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_event_category_id
        from {{ params.param_hr_core_dataset_name }}.ref_submission_event_category
        group by submission_event_category_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_submission_event_category');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;