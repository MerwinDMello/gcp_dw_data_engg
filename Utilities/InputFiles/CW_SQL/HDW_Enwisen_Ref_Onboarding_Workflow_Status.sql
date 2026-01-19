-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Ref_Onboarding_Workflow_Status.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  -- No target-dialect support for source-dialect-specific SET
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Merge the data into REF_ONBOARDING_WORKFLOW_STATUS table  */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-hr`.edwhr.ref_onboarding_workflow_status AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.workflow_status_id IS NOT NULL THEN tgt_0.workflow_status_id
          ELSE (
            SELECT
                coalesce(max(workflow_status_id), CAST(0 as BIGNUMERIC))
              FROM
                `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_workflow_status
          ) + CAST(row_number() OVER (ORDER BY workflow_status_id, stg.workflow_status_text) as BIGNUMERIC)
        END AS workflow_status_id,
        stg.workflow_status_text AS workflow_status_text,
        stg.source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(workflowstatus) AS workflow_status_text,
              'W' AS source_system_code
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_audit
            WHERE trim(workflowstatus) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_workflow_status AS tgt_0 ON tgt_0.workflow_status_text = stg.workflow_status_text
         AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
  ) AS src
  ON tgt.workflow_status_id = src.workflow_status_id
   AND upper(tgt.source_system_code) = upper(src.source_system_code)
     WHEN MATCHED THEN UPDATE SET
      dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (workflow_status_id, workflow_status_text, source_system_code, dw_last_update_date_time)
      VALUES (src.workflow_status_id, src.workflow_status_text, src.source_system_code, src.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Collect Statistics on the Target Table    */
CALL dbadmin_procs.collect_stats_table('EDWHR', 'REF_ONBOARDING_WORKFLOW_STATUS');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
