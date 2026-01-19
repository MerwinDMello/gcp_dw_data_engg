-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_patient_id_output.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Patient_ID_Output		   	   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_staging.cdm_synthesys_patientid_results	   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_ID_OUTPUT;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CDM_Synthesys_PatientID_Results');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr_staging.cdm_synthesys_patientid_results WHERE cdm_synthesys_patientid_results.unique_message_id IS NULL;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_output_wrk;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_output_wrk (cancer_patient_id_output_sk, message_control_id_text, coid, company_code, patient_dw_id, pat_acct_num, medical_record_num, patient_market_urn, last_name, first_name, birth_date, gender_code, social_security_num, address_line_1_text, address_line_2_text, city_name, state_code, zip_code, patient_type_code, message_type_code, message_event_type_code, message_flag_code, message_origin_requested_date_time, message_signed_observation_date_time, message_ingestion_date_time, message_created_date_time, document_type_id_text, document_type_text, document_type_coding_system_code, first_insert_date_time, icd_oncology_type_code, predicted_primary_icd_oncology_code, predicted_primary_icd_site_desc, suggested_primary_icd_oncology_code, suggested_primary_icd_site_desc, submitted_primary_icd_oncology_code, submitted_primary_icd_site_desc, transition_of_care_num, user_action_desc, user_action_criticality, user_action_date_time, user_3_4_id, report_assigned_date_time, attending_physician_full_name, pcp_full_name, pcp_phone_num, facility_menmonic_cs, network_mnemonic_cs, model_predicted_sarcoma_ind, submitted_sarcoma_ind, suggested_sarcoma_ind, benign_brain_tumor_type_ind, met_to_brain_tumor_type_ind, other_tumor_type_ind, source_system_code, dw_last_update_date_time)
    SELECT
        row_number() OVER (ORDER BY upper(max(stg.unique_message_id))) AS cancer_patient_id_output_sk,
        substr(max(stg.unique_message_id), 1, 50) AS unique_message_id,
        max(nullif(CASE
          WHEN length(trim(stg.coid)) = 4
           AND trim(stg.coid) IS NOT NULL THEN concat('0', trim(stg.coid))
          ELSE trim(stg.coid)
        END, '')) AS coid,
        'H' AS company_code,
        ca.patient_dw_id,
        ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(nullif(format('%#14.0f', ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(translate(stg.patient_account_number, translate(stg.patient_account_number, '0123456789', ''), '')) as NUMERIC), 0, 'ROUND_HALF_EVEN')), '')) as NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
        max(nullif(stg.patient_mrn, '')),
        substr(max(nullif(stg.patient_urn, '')), 1, 30),
        substr(max(nullif(stg.patient_last_name, '')), 1, 50),
        substr(max(nullif(stg.patient_first_name, '')), 1, 50),
        parse_date('%Y%m%d', replace(nullif(stg.patient_dob, ''), '^', '')),
        max(nullif(stg.patient_gender, '')),
        max(nullif(stg.social_security_number, '')),
        max(nullif(stg.patient_address_1, '')),
        max(nullif(stg.patient_address_2, '')),
        substr(max(nullif(stg.patient_city, '')), 1, 50),
        max(nullif(stg.patient_state, '')),
        substr(max(nullif(stg.patient_zip_code, '')), 1, 10),
        max(nullif(stg.patient_type_status, '')),
        max(nullif(stg.message_type, '')),
        max(nullif(stg.message_event_type, '')),
        max(nullif(stg.message_flag, '')),
        -- CASE WHEN STG.MESSAGE_ORIGIN_OR_REQUESTED_DATE_TIME = '' THEN NULL ELSE CAST(RPAD(STG.MESSAGE_ORIGIN_OR_REQUESTED_DATE_TIME,12,'0') AS TIMESTAMP(0) FORMAT'YYYYMMDDHHMI' ) END AS MESSAGE_ORIGIN_OR_REQUESTED_DATE_TIME,
        CASE
          WHEN upper(trim(coalesce(stg.message_origin_or_requested_date_time, ''))) = '' THEN CAST(NULL as DATETIME)
          WHEN length(trim(stg.message_origin_or_requested_date_time)) = 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.message_origin_or_requested_date_time), '000000'))
          WHEN length(trim(stg.message_origin_or_requested_date_time)) = 12 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.message_origin_or_requested_date_time), '00'))
          WHEN length(trim(stg.message_origin_or_requested_date_time)) >= 14 THEN parse_datetime('%Y%m%d%H%M%S', substr(trim(stg.message_origin_or_requested_date_time), 1, 14))
          ELSE CAST(NULL as DATETIME)
        END AS message_origin_or_requested_date_time,
        -- CASE WHEN STG.MESSAGE_SIGNED_OR_OBSERVATION_DATE_TIME= '' THEN NULL ELSE CAST(RPAD(STG.MESSAGE_SIGNED_OR_OBSERVATION_DATE_TIME,12,'0') AS TIMESTAMP(0) FORMAT'YYYYMMDDHHMI' ) END AS MESSAGE_SIGNED_OR_OBSERVATION_DATE_TIME,
        CASE
          WHEN upper(trim(coalesce(stg.message_signed_or_observation_date_time, ''))) = '' THEN CAST(NULL as DATETIME)
          WHEN length(trim(stg.message_signed_or_observation_date_time)) = 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.message_signed_or_observation_date_time), '000000'))
          WHEN length(trim(stg.message_signed_or_observation_date_time)) = 12 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.message_signed_or_observation_date_time), '00'))
          WHEN length(trim(stg.message_signed_or_observation_date_time)) >= 14 THEN parse_datetime('%Y%m%d%H%M%S', substr(trim(stg.message_signed_or_observation_date_time), 1, 14))
          ELSE CAST(NULL as DATETIME)
        END AS message_signed_or_observation_date_time,
        -- CASE WHEN STG.INGESTION_DATE_TIME='' THEN NULL ELSE CAST(SUBSTR(STG.INGESTION_DATE_TIME,1,10) || ' ' || SUBSTR(STG.INGESTION_DATE_TIME,12,8) AS TIMESTAMP(0)) END AS INGESTION_DATE_TIME,
        CASE
          WHEN upper(trim(coalesce(stg.ingestion_date_time, ''))) = '' THEN CAST(NULL as DATETIME)
          ELSE CAST(trim(concat(substr(stg.ingestion_date_time, 1, 10), ' ', substr(stg.ingestion_date_time, 12, 8))) as DATETIME)
        END AS ingestion_date_time,
        -- CASE WHEN STG.MESSAGE_CREATED_DATETIME= '' THEN NULL ELSE CAST(RPAD(STG.MESSAGE_CREATED_DATETIME,12,'0') AS TIMESTAMP(0) FORMAT'YYYYMMDDHHMI' ) END AS MESSAGE_CREATED_DATETIME,
        CASE
          WHEN upper(trim(coalesce(stg.message_created_datetime, ''))) = '' THEN CAST(NULL as DATETIME)
          WHEN length(trim(stg.message_created_datetime)) = 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.message_created_datetime), '000000'))
          WHEN length(trim(stg.message_created_datetime)) = 12 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.message_created_datetime), '00'))
          WHEN length(trim(stg.message_created_datetime)) >= 14 THEN parse_datetime('%Y%m%d%H%M%S', substr(trim(stg.message_created_datetime), 1, 14))
          ELSE CAST(NULL as DATETIME)
        END AS message_created_datetime,
        substr(max(nullif(stg.document_type_identifier, '')), 1, 20),
        substr(max(nullif(stg.document_type_text, '')), 1, 50),
        substr(max(nullif(stg.document_type_name_of_coding_sys, '')), 1, 20),
        -- CASE WHEN STG.ETL_FIRSTINSERT_DATETIME='' THEN NULL ELSE CAST(CAST(STG.ETL_FIRSTINSERT_DATETIME AS CHAR(14)) AS TIMESTAMP(0) FORMAT'YYYYMMDDHHMISS' ) END AS ETL_FIRSTINSERT_DATETIME,
        CASE
          WHEN upper(trim(coalesce(stg.etl_firstinsert_datetime, ''))) = '' THEN CAST(NULL as DATETIME)
          WHEN length(trim(stg.etl_firstinsert_datetime)) = 8 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.etl_firstinsert_datetime), '000000'))
          WHEN length(trim(stg.etl_firstinsert_datetime)) = 12 THEN parse_datetime('%Y%m%d%H%M%S', concat(trim(stg.etl_firstinsert_datetime), '00'))
          WHEN length(trim(stg.etl_firstinsert_datetime)) >= 14 THEN parse_datetime('%Y%m%d%H%M%S', substr(trim(stg.etl_firstinsert_datetime), 1, 14))
          ELSE CAST(NULL as DATETIME)
        END AS etl_firstinsert_datetime,
        '3' AS icd_oncology_type_code,
        max(nullif(replace(stg.model_predicted_primary_site_icdo3, '.', ''), '')) AS model_predicted_primary_site_icdo3,
        substr(max(nullif(stg.model_predicted_primary_site, '')), 1, 255),
        max(nullif(replace(stg.suggested_primary_site_icdo3, '.', ''), '')) AS suggested_primary_site_icdo3,
        substr(max(nullif(stg.suggested_primary_site, '')), 1, 255),
        max(nullif(replace(stg.submitted_primary_site_icdo3, '.', ''), '')) AS submitted_primary_site_icdo3,
        substr(max(nullif(stg.submitted_primary_site, '')), 1, 255),
        CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg.transition_of_care) as INT64) AS transition_of_care,
        substr(max(nullif(stg.user_action, '')), 1, 20),
        max(nullif(stg.user_action_criticality, '')),
        -- CASE WHEN STG.USER_ACTION_DATE_TIME='' THEN NULL ELSE CAST(SUBSTR(STG.USER_ACTION_DATE_TIME,1,10) || ' ' || SUBSTR(STG.USER_ACTION_DATE_TIME,12,8) AS TIMESTAMP(0)) END AS USER_ACTION_DATE_TIME,
        CASE
          WHEN upper(trim(coalesce(stg.user_action_date_time, ''))) = '' THEN CAST(NULL as DATETIME)
          ELSE CAST(trim(concat(substr(stg.user_action_date_time, 1, 10), ' ', substr(stg.user_action_date_time, 12, 8))) as DATETIME)
        END AS user_action_date_time,
        max(nullif(stg.usr, '')),
        -- CASE WHEN STG.REPORT_ASSIGNED_TO_USER_AT='' THEN NULL ELSE CAST(SUBSTR(STG.REPORT_ASSIGNED_TO_USER_AT,1,10) || ' ' || SUBSTR(STG.REPORT_ASSIGNED_TO_USER_AT,12,8) AS TIMESTAMP(0)) END AS REPORT_ASSIGNED_TO_USER_AT,
        CASE
          WHEN upper(trim(coalesce(stg.report_assigned_to_user_at, ''))) = '' THEN CAST(NULL as DATETIME)
          ELSE CAST(trim(concat(substr(stg.report_assigned_to_user_at, 1, 10), ' ', substr(stg.report_assigned_to_user_at, 12, 8))) as DATETIME)
        END AS report_assigned_to_user_at,
        substr(max(nullif(trim(stg.attending_physician), '')), 1, 100) AS attending_physician,
        substr(max(nullif(trim(stg.primary_care_physician), '')), 1, 100) AS primary_care_physician,
        substr(max(nullif(stg.pcp_phone, '')), 1, 15),
        nullif(stg.facility_mnemonic, ''),
        nullif(stg.meditech_network_id, ''),
        max(CASE
          WHEN upper(rtrim(stg.model_predicted_sarcoma)) = 'SARCOMA' THEN 'Y'
          ELSE 'N'
        END) AS model_predicted_sarcoma_ind,
        max(CASE
          WHEN upper(rtrim(stg.submitted_sarcoma)) = 'SARCOMA' THEN 'Y'
          ELSE 'N'
        END) AS submitted_sarcoma_ind,
        max(CASE
          WHEN upper(rtrim(stg.suggested_sarcoma)) = 'SARCOMA' THEN 'Y'
          ELSE 'N'
        END) AS suggested_sarcoma_ind,
        max(CASE
           upper(rtrim(stg.tumor_type_benign_brain))
          WHEN 'TRUE' THEN 'Y'
          WHEN 'FALSE' THEN 'N'
          ELSE CAST(NULL as STRING)
        END) AS benign_brain_tumor_type_ind,
        max(CASE
           upper(rtrim(stg.tumor_type_met_to_brain))
          WHEN 'TRUE' THEN 'Y'
          WHEN 'FALSE' THEN 'N'
          ELSE CAST(NULL as STRING)
        END) AS met_to_brain_tumor_type_ind,
        max(CASE
           upper(rtrim(stg.tumor_type_na))
          WHEN 'TRUE' THEN 'Y'
          WHEN 'FALSE' THEN 'N'
          ELSE CAST(NULL as STRING)
        END) AS other_tumor_type_ind,
        'H' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cdm_synthesys_patientid_results AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.clinical_acctkeys AS ca ON ca.pat_acct_num = ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(translate(stg.patient_account_number, translate(stg.patient_account_number, '0123456789', ''), '')) as NUMERIC), 0, 'ROUND_HALF_EVEN')
         AND upper(rtrim(ca.coid)) = upper(rtrim(CASE
          WHEN length(trim(stg.coid)) = 4
           AND trim(stg.coid) IS NOT NULL THEN concat('0', trim(stg.coid))
          ELSE trim(stg.coid)
        END))
      WHERE stg.unique_message_id IS NOT NULL
      GROUP BY upper(substr(stg.unique_message_id, 1, 50)), upper(nullif(CASE
        WHEN length(trim(stg.coid)) = 4
         AND trim(stg.coid) IS NOT NULL THEN concat('0', trim(stg.coid))
        ELSE trim(stg.coid)
      END, '')), 5, 6, upper(nullif(stg.patient_mrn, '')), upper(substr(nullif(stg.patient_urn, ''), 1, 30)), upper(substr(nullif(stg.patient_last_name, ''), 1, 50)), upper(substr(nullif(stg.patient_first_name, ''), 1, 50)), 11, upper(nullif(stg.patient_gender, '')), upper(nullif(stg.social_security_number, '')), upper(nullif(stg.patient_address_1, '')), upper(nullif(stg.patient_address_2, '')), upper(substr(nullif(stg.patient_city, ''), 1, 50)), upper(nullif(stg.patient_state, '')), upper(substr(nullif(stg.patient_zip_code, ''), 1, 10)), upper(nullif(stg.patient_type_status, '')), upper(nullif(stg.message_type, '')), upper(nullif(stg.message_event_type, '')), upper(nullif(stg.message_flag, '')), 23, 24, 25, 26, upper(substr(nullif(stg.document_type_identifier, ''), 1, 20)), upper(substr(nullif(stg.document_type_text, ''), 1, 50)), upper(substr(nullif(stg.document_type_name_of_coding_sys, ''), 1, 20)), 30, upper(nullif(replace(stg.model_predicted_primary_site_icdo3, '.', ''), '')), upper(substr(nullif(stg.model_predicted_primary_site, ''), 1, 255)), upper(nullif(replace(stg.suggested_primary_site_icdo3, '.', ''), '')), upper(substr(nullif(stg.suggested_primary_site, ''), 1, 255)), upper(nullif(replace(stg.submitted_primary_site_icdo3, '.', ''), '')), upper(substr(nullif(stg.submitted_primary_site, ''), 1, 255)), 38, upper(substr(nullif(stg.user_action, ''), 1, 20)), upper(nullif(stg.user_action_criticality, '')), 41, upper(nullif(stg.usr, '')), 43, upper(substr(nullif(trim(stg.attending_physician), ''), 1, 100)), upper(substr(nullif(trim(stg.primary_care_physician), ''), 1, 100)), upper(substr(nullif(stg.pcp_phone, ''), 1, 15)), 47, 48, upper(CASE
        WHEN upper(rtrim(stg.model_predicted_sarcoma)) = 'SARCOMA' THEN 'Y'
        ELSE 'N'
      END), upper(CASE
        WHEN upper(rtrim(stg.submitted_sarcoma)) = 'SARCOMA' THEN 'Y'
        ELSE 'N'
      END), upper(CASE
        WHEN upper(rtrim(stg.suggested_sarcoma)) = 'SARCOMA' THEN 'Y'
        ELSE 'N'
      END), upper(CASE
         upper(rtrim(stg.tumor_type_benign_brain))
        WHEN 'TRUE' THEN 'Y'
        WHEN 'FALSE' THEN 'N'
        ELSE CAST(NULL as STRING)
      END), upper(CASE
         upper(rtrim(stg.tumor_type_met_to_brain))
        WHEN 'TRUE' THEN 'Y'
        WHEN 'FALSE' THEN 'N'
        ELSE CAST(NULL as STRING)
      END), upper(CASE
         upper(rtrim(stg.tumor_type_na))
        WHEN 'TRUE' THEN 'Y'
        WHEN 'FALSE' THEN 'N'
        ELSE CAST(NULL as STRING)
      END)
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
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_output;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_output AS mt USING (
    SELECT DISTINCT
        cancer_patient_id_output_wrk.cancer_patient_id_output_sk,
        cancer_patient_id_output_wrk.message_control_id_text,
        cancer_patient_id_output_wrk.coid AS coid,
        cancer_patient_id_output_wrk.company_code AS company_code,
        cancer_patient_id_output_wrk.patient_dw_id,
        cancer_patient_id_output_wrk.pat_acct_num,
        cancer_patient_id_output_wrk.medical_record_num AS medical_record_num,
        cancer_patient_id_output_wrk.patient_market_urn,
        cancer_patient_id_output_wrk.last_name,
        cancer_patient_id_output_wrk.first_name,
        cancer_patient_id_output_wrk.birth_date,
        cancer_patient_id_output_wrk.gender_code AS gender_code,
        cancer_patient_id_output_wrk.social_security_num AS social_security_num,
        cancer_patient_id_output_wrk.address_line_1_text,
        cancer_patient_id_output_wrk.address_line_2_text,
        cancer_patient_id_output_wrk.city_name,
        cancer_patient_id_output_wrk.state_code AS state_code,
        cancer_patient_id_output_wrk.zip_code,
        cancer_patient_id_output_wrk.patient_type_code AS patient_type_code,
        cancer_patient_id_output_wrk.message_type_code AS message_type_code,
        cancer_patient_id_output_wrk.message_event_type_code AS message_event_type_code,
        cancer_patient_id_output_wrk.message_flag_code AS message_flag_code,
        cancer_patient_id_output_wrk.message_origin_requested_date_time,
        cancer_patient_id_output_wrk.message_signed_observation_date_time,
        cancer_patient_id_output_wrk.message_ingestion_date_time,
        cancer_patient_id_output_wrk.message_created_date_time,
        cancer_patient_id_output_wrk.document_type_id_text,
        cancer_patient_id_output_wrk.document_type_text,
        cancer_patient_id_output_wrk.document_type_coding_system_code,
        cancer_patient_id_output_wrk.first_insert_date_time,
        cancer_patient_id_output_wrk.icd_oncology_type_code AS icd_oncology_type_code,
        cancer_patient_id_output_wrk.predicted_primary_icd_oncology_code AS predicted_primary_icd_oncology_code,
        cancer_patient_id_output_wrk.predicted_primary_icd_site_desc,
        cancer_patient_id_output_wrk.suggested_primary_icd_oncology_code AS suggested_primary_icd_oncology_code,
        cancer_patient_id_output_wrk.suggested_primary_icd_site_desc,
        cancer_patient_id_output_wrk.submitted_primary_icd_oncology_code AS submitted_primary_icd_oncology_code,
        cancer_patient_id_output_wrk.submitted_primary_icd_site_desc,
        cancer_patient_id_output_wrk.transition_of_care_num,
        cancer_patient_id_output_wrk.user_action_desc,
        cancer_patient_id_output_wrk.user_action_criticality,
        cancer_patient_id_output_wrk.user_action_date_time,
        cancer_patient_id_output_wrk.user_3_4_id AS user_3_4_id,
        cancer_patient_id_output_wrk.report_assigned_date_time,
        cancer_patient_id_output_wrk.attending_physician_full_name,
        cancer_patient_id_output_wrk.pcp_full_name,
        cancer_patient_id_output_wrk.pcp_phone_num,
        cancer_patient_id_output_wrk.facility_menmonic_cs AS facility_menmonic_cs,
        substr(cancer_patient_id_output_wrk.network_mnemonic_cs, 1, 10) AS network_mnemonic_cs,
        cancer_patient_id_output_wrk.model_predicted_sarcoma_ind AS model_predicted_sarcoma_ind,
        cancer_patient_id_output_wrk.submitted_sarcoma_ind AS submitted_sarcoma_ind,
        cancer_patient_id_output_wrk.suggested_sarcoma_ind AS suggested_sarcoma_ind,
        cancer_patient_id_output_wrk.benign_brain_tumor_type_ind AS benign_brain_tumor_type_ind,
        cancer_patient_id_output_wrk.met_to_brain_tumor_type_ind AS met_to_brain_tumor_type_ind,
        cancer_patient_id_output_wrk.other_tumor_type_ind AS other_tumor_type_ind,
        cancer_patient_id_output_wrk.source_system_code AS source_system_code,
        cancer_patient_id_output_wrk.dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cancer_patient_id_output_wrk
  ) AS ms
  ON mt.cancer_patient_id_output_sk = ms.cancer_patient_id_output_sk
   AND (upper(coalesce(mt.message_control_id_text, '0')) = upper(coalesce(ms.message_control_id_text, '0'))
   AND upper(coalesce(mt.message_control_id_text, '1')) = upper(coalesce(ms.message_control_id_text, '1')))
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
   AND (upper(coalesce(mt.last_name, '0')) = upper(coalesce(ms.last_name, '0'))
   AND upper(coalesce(mt.last_name, '1')) = upper(coalesce(ms.last_name, '1')))
   AND (upper(coalesce(mt.first_name, '0')) = upper(coalesce(ms.first_name, '0'))
   AND upper(coalesce(mt.first_name, '1')) = upper(coalesce(ms.first_name, '1')))
   AND (coalesce(mt.birth_date, DATE '1970-01-01') = coalesce(ms.birth_date, DATE '1970-01-01')
   AND coalesce(mt.birth_date, DATE '1970-01-02') = coalesce(ms.birth_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.gender_code, '0')) = upper(coalesce(ms.gender_code, '0'))
   AND upper(coalesce(mt.gender_code, '1')) = upper(coalesce(ms.gender_code, '1')))
   AND (upper(coalesce(mt.social_security_num, '0')) = upper(coalesce(ms.social_security_num, '0'))
   AND upper(coalesce(mt.social_security_num, '1')) = upper(coalesce(ms.social_security_num, '1')))
   AND (upper(coalesce(mt.address_line_1_text, '0')) = upper(coalesce(ms.address_line_1_text, '0'))
   AND upper(coalesce(mt.address_line_1_text, '1')) = upper(coalesce(ms.address_line_1_text, '1')))
   AND (upper(coalesce(mt.address_line_2_text, '0')) = upper(coalesce(ms.address_line_2_text, '0'))
   AND upper(coalesce(mt.address_line_2_text, '1')) = upper(coalesce(ms.address_line_2_text, '1')))
   AND (upper(coalesce(mt.city_name, '0')) = upper(coalesce(ms.city_name, '0'))
   AND upper(coalesce(mt.city_name, '1')) = upper(coalesce(ms.city_name, '1')))
   AND (upper(coalesce(mt.state_code, '0')) = upper(coalesce(ms.state_code, '0'))
   AND upper(coalesce(mt.state_code, '1')) = upper(coalesce(ms.state_code, '1')))
   AND (upper(coalesce(mt.zip_code, '0')) = upper(coalesce(ms.zip_code, '0'))
   AND upper(coalesce(mt.zip_code, '1')) = upper(coalesce(ms.zip_code, '1')))
   AND (upper(coalesce(mt.patient_type_code, '0')) = upper(coalesce(ms.patient_type_code, '0'))
   AND upper(coalesce(mt.patient_type_code, '1')) = upper(coalesce(ms.patient_type_code, '1')))
   AND (upper(coalesce(mt.message_type_code, '0')) = upper(coalesce(ms.message_type_code, '0'))
   AND upper(coalesce(mt.message_type_code, '1')) = upper(coalesce(ms.message_type_code, '1')))
   AND (upper(coalesce(mt.message_event_type_code, '0')) = upper(coalesce(ms.message_event_type_code, '0'))
   AND upper(coalesce(mt.message_event_type_code, '1')) = upper(coalesce(ms.message_event_type_code, '1')))
   AND (upper(coalesce(mt.message_flag_code, '0')) = upper(coalesce(ms.message_flag_code, '0'))
   AND upper(coalesce(mt.message_flag_code, '1')) = upper(coalesce(ms.message_flag_code, '1')))
   AND (coalesce(mt.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_origin_requested_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_signed_observation_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_ingestion_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_ingestion_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_ingestion_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_ingestion_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (coalesce(mt.message_created_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.message_created_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.message_created_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.message_created_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.document_type_id_text, '0')) = upper(coalesce(ms.document_type_id_text, '0'))
   AND upper(coalesce(mt.document_type_id_text, '1')) = upper(coalesce(ms.document_type_id_text, '1')))
   AND (upper(coalesce(mt.document_type_text, '0')) = upper(coalesce(ms.document_type_text, '0'))
   AND upper(coalesce(mt.document_type_text, '1')) = upper(coalesce(ms.document_type_text, '1')))
   AND (upper(coalesce(mt.document_type_coding_system_code, '0')) = upper(coalesce(ms.document_type_coding_system_code, '0'))
   AND upper(coalesce(mt.document_type_coding_system_code, '1')) = upper(coalesce(ms.document_type_coding_system_code, '1')))
   AND (coalesce(mt.first_insert_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.first_insert_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.first_insert_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.first_insert_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.icd_oncology_type_code, '0')) = upper(coalesce(ms.icd_oncology_type_code, '0'))
   AND upper(coalesce(mt.icd_oncology_type_code, '1')) = upper(coalesce(ms.icd_oncology_type_code, '1')))
   AND (upper(coalesce(mt.predicted_primary_icd_oncology_code, '0')) = upper(coalesce(ms.predicted_primary_icd_oncology_code, '0'))
   AND upper(coalesce(mt.predicted_primary_icd_oncology_code, '1')) = upper(coalesce(ms.predicted_primary_icd_oncology_code, '1')))
   AND (upper(coalesce(mt.predicted_primary_icd_site_desc, '0')) = upper(coalesce(ms.predicted_primary_icd_site_desc, '0'))
   AND upper(coalesce(mt.predicted_primary_icd_site_desc, '1')) = upper(coalesce(ms.predicted_primary_icd_site_desc, '1')))
   AND (upper(coalesce(mt.suggested_primary_icd_oncology_code, '0')) = upper(coalesce(ms.suggested_primary_icd_oncology_code, '0'))
   AND upper(coalesce(mt.suggested_primary_icd_oncology_code, '1')) = upper(coalesce(ms.suggested_primary_icd_oncology_code, '1')))
   AND (upper(coalesce(mt.suggested_primary_icd_site_desc, '0')) = upper(coalesce(ms.suggested_primary_icd_site_desc, '0'))
   AND upper(coalesce(mt.suggested_primary_icd_site_desc, '1')) = upper(coalesce(ms.suggested_primary_icd_site_desc, '1')))
   AND (upper(coalesce(mt.submitted_primary_icd_oncology_code, '0')) = upper(coalesce(ms.submitted_primary_icd_oncology_code, '0'))
   AND upper(coalesce(mt.submitted_primary_icd_oncology_code, '1')) = upper(coalesce(ms.submitted_primary_icd_oncology_code, '1')))
   AND (upper(coalesce(mt.submitted_primary_icd_site_desc, '0')) = upper(coalesce(ms.submitted_primary_icd_site_desc, '0'))
   AND upper(coalesce(mt.submitted_primary_icd_site_desc, '1')) = upper(coalesce(ms.submitted_primary_icd_site_desc, '1')))
   AND (coalesce(mt.transition_of_care_num, 0) = coalesce(ms.transition_of_care_num, 0)
   AND coalesce(mt.transition_of_care_num, 1) = coalesce(ms.transition_of_care_num, 1))
   AND (upper(coalesce(mt.user_action_desc, '0')) = upper(coalesce(ms.user_action_desc, '0'))
   AND upper(coalesce(mt.user_action_desc, '1')) = upper(coalesce(ms.user_action_desc, '1')))
   AND (upper(coalesce(mt.user_action_criticality_text, '0')) = upper(coalesce(ms.user_action_criticality, '0'))
   AND upper(coalesce(mt.user_action_criticality_text, '1')) = upper(coalesce(ms.user_action_criticality, '1')))
   AND (coalesce(mt.user_action_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.user_action_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.user_action_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.user_action_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.user_3_4_id, '0')) = upper(coalesce(ms.user_3_4_id, '0'))
   AND upper(coalesce(mt.user_3_4_id, '1')) = upper(coalesce(ms.user_3_4_id, '1')))
   AND (coalesce(mt.report_assigned_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.report_assigned_date_time, DATETIME '1970-01-01 00:00:00')
   AND coalesce(mt.report_assigned_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.report_assigned_date_time, DATETIME '1970-01-01 00:00:01'))
   AND (upper(coalesce(mt.attending_physician_full_name, '0')) = upper(coalesce(ms.attending_physician_full_name, '0'))
   AND upper(coalesce(mt.attending_physician_full_name, '1')) = upper(coalesce(ms.attending_physician_full_name, '1')))
   AND (upper(coalesce(mt.pcp_full_name, '0')) = upper(coalesce(ms.pcp_full_name, '0'))
   AND upper(coalesce(mt.pcp_full_name, '1')) = upper(coalesce(ms.pcp_full_name, '1')))
   AND (upper(coalesce(mt.pcp_phone_num, '0')) = upper(coalesce(ms.pcp_phone_num, '0'))
   AND upper(coalesce(mt.pcp_phone_num, '1')) = upper(coalesce(ms.pcp_phone_num, '1')))
   AND (coalesce(mt.facility_menmonic_cs, '0') = coalesce(ms.facility_menmonic_cs, '0')
   AND coalesce(mt.facility_menmonic_cs, '1') = coalesce(ms.facility_menmonic_cs, '1'))
   AND (coalesce(mt.network_mnemonic_cs, '0') = coalesce(ms.network_mnemonic_cs, '0')
   AND coalesce(mt.network_mnemonic_cs, '1') = coalesce(ms.network_mnemonic_cs, '1'))
   AND (upper(coalesce(mt.model_predicted_sarcoma_ind, '0')) = upper(coalesce(ms.model_predicted_sarcoma_ind, '0'))
   AND upper(coalesce(mt.model_predicted_sarcoma_ind, '1')) = upper(coalesce(ms.model_predicted_sarcoma_ind, '1')))
   AND (upper(coalesce(mt.submitted_sarcoma_ind, '0')) = upper(coalesce(ms.submitted_sarcoma_ind, '0'))
   AND upper(coalesce(mt.submitted_sarcoma_ind, '1')) = upper(coalesce(ms.submitted_sarcoma_ind, '1')))
   AND (upper(coalesce(mt.suggested_sarcoma_ind, '0')) = upper(coalesce(ms.suggested_sarcoma_ind, '0'))
   AND upper(coalesce(mt.suggested_sarcoma_ind, '1')) = upper(coalesce(ms.suggested_sarcoma_ind, '1')))
   AND (upper(coalesce(mt.benign_brain_tumor_type_ind, '0')) = upper(coalesce(ms.benign_brain_tumor_type_ind, '0'))
   AND upper(coalesce(mt.benign_brain_tumor_type_ind, '1')) = upper(coalesce(ms.benign_brain_tumor_type_ind, '1')))
   AND (upper(coalesce(mt.met_to_brain_tumor_type_ind, '0')) = upper(coalesce(ms.met_to_brain_tumor_type_ind, '0'))
   AND upper(coalesce(mt.met_to_brain_tumor_type_ind, '1')) = upper(coalesce(ms.met_to_brain_tumor_type_ind, '1')))
   AND (upper(coalesce(mt.other_tumor_type_ind, '0')) = upper(coalesce(ms.other_tumor_type_ind, '0'))
   AND upper(coalesce(mt.other_tumor_type_ind, '1')) = upper(coalesce(ms.other_tumor_type_ind, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cancer_patient_id_output_sk, message_control_id_text, coid, company_code, patient_dw_id, pat_acct_num, medical_record_num, patient_market_urn, last_name, first_name, birth_date, gender_code, social_security_num, address_line_1_text, address_line_2_text, city_name, state_code, zip_code, patient_type_code, message_type_code, message_event_type_code, message_flag_code, message_origin_requested_date_time, message_signed_observation_date_time, message_ingestion_date_time, message_created_date_time, document_type_id_text, document_type_text, document_type_coding_system_code, first_insert_date_time, icd_oncology_type_code, predicted_primary_icd_oncology_code, predicted_primary_icd_site_desc, suggested_primary_icd_oncology_code, suggested_primary_icd_site_desc, submitted_primary_icd_oncology_code, submitted_primary_icd_site_desc, transition_of_care_num, user_action_desc, user_action_criticality_text, user_action_date_time, user_3_4_id, report_assigned_date_time, attending_physician_full_name, pcp_full_name, pcp_phone_num, facility_menmonic_cs, network_mnemonic_cs, model_predicted_sarcoma_ind, submitted_sarcoma_ind, suggested_sarcoma_ind, benign_brain_tumor_type_ind, met_to_brain_tumor_type_ind, other_tumor_type_ind, source_system_code, dw_last_update_date_time)
      VALUES (ms.cancer_patient_id_output_sk, ms.message_control_id_text, ms.coid, ms.company_code, ms.patient_dw_id, ms.pat_acct_num, ms.medical_record_num, ms.patient_market_urn, ms.last_name, ms.first_name, ms.birth_date, ms.gender_code, ms.social_security_num, ms.address_line_1_text, ms.address_line_2_text, ms.city_name, ms.state_code, ms.zip_code, ms.patient_type_code, ms.message_type_code, ms.message_event_type_code, ms.message_flag_code, ms.message_origin_requested_date_time, ms.message_signed_observation_date_time, ms.message_ingestion_date_time, ms.message_created_date_time, ms.document_type_id_text, ms.document_type_text, ms.document_type_coding_system_code, ms.first_insert_date_time, ms.icd_oncology_type_code, ms.predicted_primary_icd_oncology_code, ms.predicted_primary_icd_site_desc, ms.suggested_primary_icd_oncology_code, ms.suggested_primary_icd_site_desc, ms.submitted_primary_icd_oncology_code, ms.submitted_primary_icd_site_desc, ms.transition_of_care_num, ms.user_action_desc, ms.user_action_criticality, ms.user_action_date_time, ms.user_3_4_id, ms.report_assigned_date_time, ms.attending_physician_full_name, ms.pcp_full_name, ms.pcp_phone_num, ms.facility_menmonic_cs, ms.network_mnemonic_cs, ms.model_predicted_sarcoma_ind, ms.submitted_sarcoma_ind, ms.suggested_sarcoma_ind, ms.benign_brain_tumor_type_ind, ms.met_to_brain_tumor_type_ind, ms.other_tumor_type_ind, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- ET;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Cancer_Patient_Id_Output');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
