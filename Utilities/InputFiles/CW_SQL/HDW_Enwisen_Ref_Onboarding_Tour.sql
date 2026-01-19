-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Ref_Onboarding_Tour.sql
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
/*  Merge the data into EDWHR.REF_ONBOARDING_TOUR table  */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-hr`.edwhr.ref_onboarding_tour AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.tour_id IS NOT NULL THEN tgt_0.tour_id
          ELSE (
            SELECT
                coalesce(max(tour_id), CAST(0 as BIGNUMERIC))
              FROM
                `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_tour
          ) + CAST(row_number() OVER (ORDER BY tour_id, stg.tour_name) as BIGNUMERIC)
        END AS tour_id,
        stg.tour_name AS tour_name,
        stg.source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(tourname) AS tour_name,
              'W' AS source_system_code
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_audit
            WHERE trim(tourname) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_tour AS tgt_0 ON tgt_0.tour_name = stg.tour_name
         AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
  ) AS src
  ON tgt.tour_id = src.tour_id
   AND upper(tgt.source_system_code) = upper(src.source_system_code)
     WHEN MATCHED THEN UPDATE SET
      dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (tour_id, tour_name, source_system_code, dw_last_update_date_time)
      VALUES (src.tour_id, src.tour_name, src.source_system_code, src.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Collect Statistics on the Target Table    */
CALL dbadmin_procs.collect_stats_table('EDWHR', 'REF_ONBOARDING_TOUR');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
