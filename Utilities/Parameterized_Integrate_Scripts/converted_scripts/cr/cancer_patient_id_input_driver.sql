-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cancer_patient_id_input_driver.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CANCER_PATIENT_ID_INPUT_DRIVER                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_CANCER_PAT_ID_INPUT_DRIVER;' FOR SESSION;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_input_driver;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_input_driver AS mt USING (
    SELECT DISTINCT
        CAST(row_number() OVER (ORDER BY sk.cancer_patient_driver_sk) as NUMERIC) AS cancer_patient_id_input_driver_sk,
        sk.cancer_patient_driver_sk,
        a.message_control_id_text,
        a.patient_type_status_sk,
        a.coid AS coid,
        a.company_code AS company_code,
        a.patient_dw_id,
        a.pat_acct_num,
        a.medical_record_num AS medical_record_num,
        a.patient_market_urn,
        a.message_type_code AS message_type_code,
        a.message_flag_code AS message_flag_code,
        a.message_event_type_code AS message_event_type_code,
        a.message_origin_requested_date_time,
        a.message_signed_observation_date_time,
        a.message_created_date_time,
        a.first_insert_date_time,
        'H' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_input AS a
        LEFT OUTER JOIN (
          SELECT
              a1.cancer_patient_driver_sk,
              a1.message_control_id_text
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output_driver AS a1
            QUALIFY row_number() OVER (PARTITION BY upper(a1.message_control_id_text) ORDER BY a1.user_action_date_time DESC) = 1
        ) AS sk ON upper(rtrim(a.message_control_id_text)) = upper(rtrim(sk.message_control_id_text))
  ) AS ms
  ON mt.cancer_patient_id_input_driver_sk = ms.cancer_patient_id_input_driver_sk
   AND (coalesce(mt.cancer_patient_driver_sk, NUMERIC '0') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '0')
   AND coalesce(mt.cancer_patient_driver_sk, NUMERIC '1') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '1'))
   AND (upper(coalesce(mt.message_control_id_text, '0')) = upper(coalesce(ms.message_control_id_text, '0'))
   AND upper(coalesce(mt.message_control_id_text, '1')) = upper(coalesce(ms.message_control_id_text, '1')))
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
      INSERT (cancer_patient_id_input_driver_sk, cancer_patient_driver_sk, message_control_id_text, patient_type_status_sk, coid, company_code, patient_dw_id, pat_acct_num, medical_record_num, patient_market_urn, message_type_code, message_flag_code, message_event_type_code, message_origin_requested_date_time, message_signed_observation_date_time, message_created_date_time, first_insert_date_time, source_system_code, dw_last_update_date_time)
      VALUES (ms.cancer_patient_id_input_driver_sk, ms.cancer_patient_driver_sk, ms.message_control_id_text, ms.patient_type_status_sk, ms.coid, ms.company_code, ms.patient_dw_id, ms.pat_acct_num, ms.medical_record_num, ms.patient_market_urn, ms.message_type_code, ms.message_flag_code, ms.message_event_type_code, ms.message_origin_requested_date_time, ms.message_signed_observation_date_time, ms.message_created_date_time, ms.first_insert_date_time, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Cancer_Patient_Id_Input_Driver');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
