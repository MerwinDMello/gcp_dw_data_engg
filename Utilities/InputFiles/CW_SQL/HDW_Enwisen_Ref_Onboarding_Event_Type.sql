-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Ref_Onboarding_Event_Type.sql
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
/*  Merge the data into EDWHR.Ref_Onboarding_Event_Type table  */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-hr`.edwhr.ref_onboarding_event_type AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.event_type_id IS NOT NULL THEN tgt_0.event_type_id
          ELSE (
            SELECT
                coalesce(max(event_type_id), CAST(0 as BIGNUMERIC))
              FROM
                `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_event_type
          ) + CAST(row_number() OVER (ORDER BY event_type_id, upper(stg.event_type_code)) as BIGNUMERIC)
        END AS event_type_id,
        stg.event_type_code AS event_type_code,
        stg.event_type_desc AS event_type_desc,
        stg.source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              'D' AS event_type_code,
              'Drug Screen Completion' AS event_type_desc,
              'W' AS source_system_code
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg
          UNION DISTINCT
          SELECT DISTINCT
              'T' AS event_type_code,
              'Tour Completion' AS event_type_desc,
              'W' AS source_system_code
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg
          UNION DISTINCT
          SELECT DISTINCT
              'B' AS event_type_code,
              'Authorized Background Check' AS event_type_desc,
              'W' AS source_system_code
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg
        ) AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_event_type AS tgt_0 ON upper(tgt_0.event_type_code) = upper(stg.event_type_code)
  ) AS src
  ON tgt.event_type_id = src.event_type_id
     WHEN MATCHED THEN UPDATE SET
      event_type_desc = src.event_type_desc, 
      dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (event_type_id, event_type_code, event_type_desc, source_system_code, dw_last_update_date_time)
      VALUES (src.event_type_id, src.event_type_code, src.event_type_desc, src.source_system_code, src.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Collect Statistics on the Target Table    */
CALL dbadmin_procs.collect_stats_table('EDWHR', 'Ref_Onboarding_Event_Type');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
