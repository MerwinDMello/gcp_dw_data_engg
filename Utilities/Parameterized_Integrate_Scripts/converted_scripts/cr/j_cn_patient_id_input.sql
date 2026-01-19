-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_patient_id_input.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Patient_ID_Input		   		   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_staging.Scri_Patient_ID_History		   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_Patient_ID_Input;' FOR SESSION;
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr_staging.scri_patient_id_history WHERE scri_patient_id_history.msh_msg_control_id IS NULL;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_Staging','Scri_Patient_ID_History');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_input_wrk;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_input_wrk AS mt USING (
    SELECT DISTINCT
        substr(a.message_control_id_text, 1, 50) AS message_control_id_text,
        a.patient_type_status_sk,
        a.coid AS coid,
        a.company_code AS company_code,
        a.patient_dw_id,
        ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(a.pid_pat_account_num) as NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
        a.pid_medical_record_num AS medical_record_num,
        substr(a.pid_medical_record_urn, 1, 30) AS patient_market_urn,
        a.msh_msg_type_message_code AS message_type_code,
        a.message_flag AS message_flag_code,
        a.msh_msg_type_trigger_event AS message_event_type_code,
        a.txa_origination_date_time,
        a.txa_transcription_date_time,
        a.message_created_date_time,
        a.etl_insert_date_time,
        a.source_system_code AS source_system_code,
        a.dw_last_update_date_time
      FROM
        (
          SELECT
              max(stg.msh_msg_control_id) AS message_control_id_text,
              coalesce(pt.patient_type_status_sk, (
                SELECT
                    ref_patient_type_status.patient_type_status_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr_base_views.ref_patient_type_status
                  WHERE upper(rtrim(ref_patient_type_status.patient_type_status_code)) = 'UNK'
              )) AS patient_type_status_sk,
              max(nullif(CASE
                WHEN length(trim(stg.coid)) = 4
                 AND trim(stg.coid) IS NOT NULL THEN concat('0', trim(stg.coid))
                ELSE trim(stg.coid)
              END, '')) AS coid,
              'H' AS company_code,
              ca.patient_dw_id,
              nullif(format('%#14.0f', ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(translate(stg.pid_pat_account_num, translate(stg.pid_pat_account_num, '0123456789', ''), '')) as NUMERIC), 0, 'ROUND_HALF_EVEN')), '') AS pid_pat_account_num,
              max(nullif(stg.pid_medical_record_num, '')) AS pid_medical_record_num,
              max(nullif(stg.pid_medical_record_urn, '')) AS pid_medical_record_urn,
              max(nullif(stg.msh_msg_type_message_code, '')) AS msh_msg_type_message_code,
              max(nullif(stg.message_flag, '')) AS message_flag,
              max(nullif(stg.msh_msg_type_trigger_event, '')) AS msh_msg_type_trigger_event,
              CASE
                WHEN length(stg.txa_origination_date_time) < 8 THEN CAST(NULL as DATETIME)
                WHEN length(stg.txa_origination_date_time) >= 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(substr(trim(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_strtok(stg.txa_origination_date_time, '^', 1)), 1, 14), substr('00000000000000', 1, 14 - length(substr(trim(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_strtok(stg.txa_origination_date_time, '^', 1)), 1, 14)))))
                ELSE CAST(NULL as DATETIME)
              END AS txa_origination_date_time,
              CASE
                WHEN upper(trim(stg.txa_transcription_date_time)) = '' THEN CAST(NULL as DATETIME)
                WHEN upper(stg.txa_transcription_date_time) NOT LIKE '1%'
                 AND upper(stg.txa_transcription_date_time) NOT LIKE '2%' THEN CAST(NULL as DATETIME)
                WHEN length(stg.txa_transcription_date_time) < 8 THEN CAST(NULL as DATETIME)
                WHEN length(stg.txa_transcription_date_time) >= 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(substr(trim(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_strtok(stg.txa_transcription_date_time, '^', 1)), 1, 14), substr('00000000000000', 1, 14 - length(substr(trim(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_strtok(stg.txa_transcription_date_time, '^', 1)), 1, 14)))))
                ELSE CAST(NULL as DATETIME)
              END AS txa_transcription_date_time,
              CASE
                WHEN upper(rtrim(stg.message_created_date_time)) = '' THEN CAST(NULL as DATETIME)
                WHEN length(stg.message_created_date_time) < 8 THEN CAST(NULL as DATETIME)
                WHEN length(stg.message_created_date_time) >= 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(substr(trim(stg.message_created_date_time), 1, 14), substr('00000000000000', 1, 14 - length(substr(trim(stg.message_created_date_time), 1, 14)))))
                ELSE CAST(NULL as DATETIME)
              END AS message_created_date_time,
              max(CASE
                WHEN upper(rtrim(stg.etl_insert_date_time)) = '' THEN CAST(NULL as DATETIME)
                WHEN length(stg.etl_insert_date_time) < 8 THEN CAST(NULL as DATETIME)
                WHEN length(stg.etl_insert_date_time) >= 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(substr(trim(stg.etl_insert_date_time), 1, 14), substr('00000000000000', 1, 14 - length(substr(trim(stg.etl_insert_date_time), 1, 14)))))
                ELSE CAST(NULL as DATETIME)
              END) AS etl_insert_date_time,
              'H' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.scri_patient_id_history AS stg
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.clinical_acctkeys AS ca ON ca.pat_acct_num = ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(translate(stg.pid_pat_account_num, translate(stg.pid_pat_account_num, '0123456789', ''), '')) as NUMERIC), 0, 'ROUND_HALF_EVEN')
               AND upper(rtrim(ca.coid)) = upper(rtrim(CASE
                WHEN length(trim(stg.coid)) = 4
                 AND trim(stg.coid) IS NOT NULL THEN concat('0', trim(stg.coid))
                ELSE trim(stg.coid)
              END))
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_patient_type_status AS pt ON upper(trim(stg.patient_type_status)) = upper(trim(pt.patient_type_status_code))
            WHERE upper(rtrim(stg.message_flag)) IN(
              'PAT', 'RAD', 'RADN'
            )
             AND stg.msh_msg_control_id IS NOT NULL
            GROUP BY upper(stg.msh_msg_control_id), 2, upper(nullif(CASE
              WHEN length(trim(stg.coid)) = 4
               AND trim(stg.coid) IS NOT NULL THEN concat('0', trim(stg.coid))
              ELSE trim(stg.coid)
            END, '')), 16, 5, 6, upper(nullif(stg.pid_medical_record_num, '')), upper(nullif(stg.pid_medical_record_urn, '')), upper(nullif(stg.msh_msg_type_message_code, '')), upper(nullif(stg.message_flag, '')), upper(nullif(stg.msh_msg_type_trigger_event, '')), 12, 13, 14, 16, 17
        ) AS a
      QUALIFY row_number() OVER (PARTITION BY upper(a.message_control_id_text) ORDER BY a.etl_insert_date_time DESC) = 1
  ) AS ms
  ON mt.message_control_id_text = ms.message_control_id_text
   AND (coalesce(mt.patient_type_status_sk, 0) = coalesce(ms.patient_type_status_sk, 0)
   AND coalesce(mt.patient_type_status_sk, 1) = coalesce(ms.patient_type_status_sk, 1))
   AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
   AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
   AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
   AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
   AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
   AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
   AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
   AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
   AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
   AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
   AND (upper(coalesce(mt.patient_market_urn, '0')) = upper(coalesce(ms.patient_market_urn, '0'))
   AND upper(coalesce(mt.patient_market_urn, '1')) = upper(coalesce(ms.patient_market_urn, '1')))
   AND (upper(coalesce(mt.message_type_code, '0')) = upper(coalesce(ms.message_type_code, '0'))
   AND upper(coalesce(mt.message_type_code, '1')) = upper(coalesce(ms.message_type_code, '1')))
   AND (upper(coalesce(mt.message_flag_code, '0')) = upper(coalesce(ms.message_flag_code, '0'))
   AND upper(coalesce(mt.message_flag_code, '1')) = upper(coalesce(ms.message_flag_code, '1')))
   AND (upper(coalesce(mt.message_event_type_code, '0')) = upper(coalesce(ms.message_event_type_code, '0'))
   AND upper(coalesce(mt.message_event_type_code, '1')) = upper(coalesce(ms.message_event_type_code, '1')))
   AND (coalesce(mt.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.txa_origination_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.txa_origination_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.txa_transcription_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.txa_transcription_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_created_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_created_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_created_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_created_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.first_insert_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.etl_insert_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.first_insert_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.etl_insert_date_time, DATETIME '1970-01-01 00:00:01'))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (message_control_id_text, patient_type_status_sk, coid, company_code, patient_dw_id, pat_acct_num, medical_record_num, patient_market_urn, message_type_code, message_flag_code, message_event_type_code, message_origin_requested_date_time, message_signed_observation_date_time, message_created_date_time, first_insert_date_time, source_system_code, dw_last_update_date_time)
      VALUES (ms.message_control_id_text, ms.patient_type_status_sk, ms.coid, ms.company_code, ms.patient_dw_id, ms.pat_acct_num, ms.medical_record_num, ms.patient_market_urn, ms.message_type_code, ms.message_flag_code, ms.message_event_type_code, ms.txa_origination_date_time, ms.txa_transcription_date_time, ms.message_created_date_time, ms.etl_insert_date_time, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- BT;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_input;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_input AS mt USING (
    SELECT DISTINCT
        cancer_patient_id_input_wrk.message_control_id_text,
        cancer_patient_id_input_wrk.patient_type_status_sk,
        cancer_patient_id_input_wrk.coid AS coid,
        cancer_patient_id_input_wrk.company_code AS company_code,
        cancer_patient_id_input_wrk.patient_dw_id,
        cancer_patient_id_input_wrk.pat_acct_num,
        cancer_patient_id_input_wrk.medical_record_num AS medical_record_num,
        cancer_patient_id_input_wrk.patient_market_urn,
        cancer_patient_id_input_wrk.message_type_code AS message_type_code,
        cancer_patient_id_input_wrk.message_flag_code AS message_flag_code,
        cancer_patient_id_input_wrk.message_event_type_code AS message_event_type_code,
        cancer_patient_id_input_wrk.message_origin_requested_date_time,
        cancer_patient_id_input_wrk.message_signed_observation_date_time,
        cancer_patient_id_input_wrk.message_created_date_time,
        cancer_patient_id_input_wrk.first_insert_date_time,
        cancer_patient_id_input_wrk.source_system_code AS source_system_code,
        cancer_patient_id_input_wrk.dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_input_wrk
  ) AS ms
  ON mt.message_control_id_text = ms.message_control_id_text
   AND (coalesce(mt.patient_type_status_sk, 0) = coalesce(ms.patient_type_status_sk, 0)
   AND coalesce(mt.patient_type_status_sk, 1) = coalesce(ms.patient_type_status_sk, 1))
   AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
   AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
   AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
   AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
   AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
   AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
   AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
   AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
   AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
   AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
   AND (upper(coalesce(mt.patient_market_urn, '0')) = upper(coalesce(ms.patient_market_urn, '0'))
   AND upper(coalesce(mt.patient_market_urn, '1')) = upper(coalesce(ms.patient_market_urn, '1')))
   AND (upper(coalesce(mt.message_type_code, '0')) = upper(coalesce(ms.message_type_code, '0'))
   AND upper(coalesce(mt.message_type_code, '1')) = upper(coalesce(ms.message_type_code, '1')))
   AND (upper(coalesce(mt.message_flag_code, '0')) = upper(coalesce(ms.message_flag_code, '0'))
   AND upper(coalesce(mt.message_flag_code, '1')) = upper(coalesce(ms.message_flag_code, '1')))
   AND (upper(coalesce(mt.message_event_type_code, '0')) = upper(coalesce(ms.message_event_type_code, '0'))
   AND upper(coalesce(mt.message_event_type_code, '1')) = upper(coalesce(ms.message_event_type_code, '1')))
   AND (coalesce(mt.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_created_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_created_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_created_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_created_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.first_insert_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.first_insert_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.first_insert_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.first_insert_date_time, DATETIME '1970-01-01 00:00:01'))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (message_control_id_text, patient_type_status_sk, coid, company_code, patient_dw_id, pat_acct_num, medical_record_num, patient_market_urn, message_type_code, message_flag_code, message_event_type_code, message_origin_requested_date_time, message_signed_observation_date_time, message_created_date_time, first_insert_date_time, source_system_code, dw_last_update_date_time)
      VALUES (ms.message_control_id_text, ms.patient_type_status_sk, ms.coid, ms.company_code, ms.patient_dw_id, ms.pat_acct_num, ms.medical_record_num, ms.patient_market_urn, ms.message_type_code, ms.message_flag_code, ms.message_event_type_code, ms.message_origin_requested_date_time, ms.message_signed_observation_date_time, ms.message_created_date_time, ms.first_insert_date_time, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- ET;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Cancer_Patient_Id_Input');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* DEL FROM edwcr_staging.Scri_Patient_ID_History;
.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE; */
-- EOF
