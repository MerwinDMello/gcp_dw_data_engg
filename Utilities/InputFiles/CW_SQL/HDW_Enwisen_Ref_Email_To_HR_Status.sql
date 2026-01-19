-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Ref_Email_To_HR_Status.sql
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
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'ENWISEN_AUDIT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Merge the data into EDWHR.REF_EMAIL_TO_HR_STATUS table  */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-hr`.edwhr.ref_email_to_hr_status AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.email_sent_status_id IS NOT NULL THEN tgt_0.email_sent_status_id
          ELSE (
            SELECT
                coalesce(max(email_sent_status_id), CAST(0 as BIGNUMERIC))
              FROM
                `hca-hin-dev-cur-hr`.edwhr_base_views.ref_email_to_hr_status
          ) + CAST(row_number() OVER (ORDER BY email_sent_status_id, stg.email_sent_status_text) as BIGNUMERIC)
        END AS email_sent_status_id,
        stg.email_sent_status_text AS email_sent_status_text,
        stg.hr_status_desc AS hr_status_desc,
        stg.source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(hrstatus) AS email_sent_status_text,
              max(CASE
                WHEN upper(hrstatus) = 'O' THEN 'Pre-Boarding Tour'
                WHEN upper(hrstatus) = 'V' THEN 'Verification Tour'
                WHEN upper(hrstatus) = 'C' THEN 'Acquisitions'
              END) AS hr_status_desc,
              'W' AS source_system_code
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_audit
            WHERE trim(hrstatus) IS NOT NULL
            GROUP BY 1, upper(CASE
              WHEN upper(hrstatus) = 'O' THEN 'Pre-Boarding Tour'
              WHEN upper(hrstatus) = 'V' THEN 'Verification Tour'
              WHEN upper(hrstatus) = 'C' THEN 'Acquisitions'
            END), 3
        ) AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_email_to_hr_status AS tgt_0 ON tgt_0.email_sent_status_text = stg.email_sent_status_text
         AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
  ) AS src
  ON tgt.email_sent_status_id = src.email_sent_status_id
   AND upper(tgt.source_system_code) = upper(src.source_system_code)
     WHEN MATCHED THEN UPDATE SET
      hr_status_desc = src.hr_status_desc, 
      dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (email_sent_status_id, email_sent_status_text, hr_status_desc, source_system_code, dw_last_update_date_time)
      VALUES (src.email_sent_status_id, src.email_sent_status_text, src.hr_status_desc, src.source_system_code, src.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Collect Statistics on the Target Table    */
CALL dbadmin_procs.collect_stats_table('EDWHR', 'REF_EMAIL_TO_HR_STATUS');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
