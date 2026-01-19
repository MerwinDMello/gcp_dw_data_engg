

BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_submission_event AS tgt USING (
    SELECT
        number AS submission_event_id,
        CAST(category_number as INT64) AS category_number,
        code,
        description,
        detaildescription,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidateselectionevent
    UNION ALL
    SELECT
        CASE
          WHEN submission_event_id IS NOT NULL THEN submission_event_id
          ELSE (
            SELECT
                coalesce(max(submission_event_id), CAST(100000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_submission_event
              WHERE upper(stg.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY submission_event_id, stg.code) as BIGNUMERIC)
        END AS submission_event_id,
        stg.category_number AS category_number,
        stg.code,
        stg.description,
        stg.detaildescription,
        stg.source_system_code AS source_system_code,
        stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              rsc.submission_event_category_id AS category_number,
              stg1.businessaction AS code,
              stg1.businessaction AS description,
              stg1.businessaction AS detaildescription,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_businessaction_bct_stg AS stg1
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category AS rsc 
              ON COALESCE(UPPER(TRIM(stg1.actionlabel)),'') = COALESCE(UPPER(TRIM(rsc.submission_event_category_code)),'')
            WHERE upper(stg1.businessview) = 'JOBAPPLICATION'
          UNION ALL
          SELECT DISTINCT
              rsc.submission_event_category_id AS category_number,
              stg2.actionname AS code,
              stg2.actionname AS description,
              stg2.actionname AS detaildescription,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_edw_useraction_stg AS stg2
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category AS rsc 
              ON COALESCE(UPPER(TRIM(stg2.type_state)),'') = COALESCE(UPPER(TRIM(rsc.submission_event_category_code)),'')
            WHERE stg2.actionname NOT IN(
              SELECT DISTINCT
                  businessaction
                FROM
                  {{ params.param_hr_stage_dataset_name }}.ats_businessaction_bct_stg
                WHERE upper(businessview) = 'JOBAPPLICATION'
            )
          UNION ALL
          SELECT DISTINCT
              --  Adding new action from ATS_JobAppWorkFlowStephis_BCT_STG
              rsc.submission_event_category_id AS category_number,
              stg3.triggeredby AS code,
              stg3.triggeredby AS description,
              stg3.triggeredby AS detaildescription,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobapplicationworkflowstephistory_stg AS stg3
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category AS rsc 
              ON COALESCE(UPPER(TRIM(stg3.action)),'') = COALESCE(UPPER(TRIM(rsc.submission_event_category_code)),'')
            WHERE stg3.triggeredby NOT IN(
              SELECT DISTINCT
                  submission_event_code
                FROM
                  {{ params.param_hr_base_views_dataset_name }}.ref_submission_event
                WHERE Upper(Trim(source_system_code)) = 'B'
            )
             AND stg3.triggeredby NOT LIKE 'Oferta%'
             AND stg3.triggeredby NOT LIKE 'Offre acceptÃƒÂ©e'
             AND stg3.triggeredby NOT LIKE 'Retirar'
             AND stg3.triggeredby NOT LIKE 'Ã¥%'
             AND stg3.triggeredby NOT LIKE 'Ã%'
        ) AS stg
        LEFT OUTER JOIN --  to exclude other languages (only english)
        {{ params.param_hr_base_views_dataset_name }}.ref_submission_event AS tgt_0 
        ON COALESCE(UPPER(TRIM(tgt_0.submission_event_code)),'') = COALESCE(UPPER(TRIM(stg.code)),'')
         AND COALESCE(UPPER(TRIM(tgt_0.source_system_code)),'') = COALESCE(UPPER(TRIM(stg.source_system_code)),'')
  ) AS src
  ON tgt.submission_event_id = src.submission_event_id
   AND tgt.source_system_code = src.source_system_code
     WHEN MATCHED THEN UPDATE SET submission_event_category_id = src.category_number, submission_event_code = src.code, submission_event_desc = src.description, submission_event_detail_desc = src.detaildescription, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (submission_event_id, submission_event_category_id, submission_event_code, submission_event_desc, submission_event_detail_desc, source_system_code, dw_last_update_date_time) VALUES (CAST(src.submission_event_id as INT64), src.category_number, src.code, src.description, src.detaildescription, src.source_system_code, DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND))
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_event_id
        from {{ params.param_hr_core_dataset_name }}.ref_submission_event
        group by submission_event_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_submission_event');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
