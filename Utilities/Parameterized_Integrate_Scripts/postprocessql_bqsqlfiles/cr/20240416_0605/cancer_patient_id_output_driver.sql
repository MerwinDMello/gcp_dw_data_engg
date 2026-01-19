DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cancer_patient_id_output_driver.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CANCER_PATIENT_ID_OUTPUT_DRIVER                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_CANCER_PAT_ID_OUTPUT_DRIVER;;
 --' FOR SESSION;;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_output_driver;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_output_driver AS mt USING
  (SELECT DISTINCT CAST(row_number() OVER (
                                           ORDER BY a1.cancer_patient_driver_sk, a1.cancer_patient_id_output_sk) AS NUMERIC) AS cancer_patient_id_output_driver_sk,
                   a1.cancer_patient_driver_sk,
                   a1.cancer_patient_id_output_sk,
                   a1.message_control_id_text,
                   a1.user_action_date_time,
                   a1.coid AS coid,
                   a1.company_code AS company_code,
                   a1.patient_dw_id,
                   a1.pat_acct_num,
                   a1.medical_record_num AS medical_record_num,
                   a1.patient_market_urn,
                   a1.last_name,
                   a1.first_name,
                   a1.birth_date,
                   a1.gender_code AS gender_code,
                   a1.social_security_num AS social_security_num,
                   a1.address_line_1_text,
                   a1.address_line_2_text,
                   a1.city_name,
                   a1.state_code AS state_code,
                   a1.zip_code,
                   a1.patient_type_code AS patient_type_code,
                   a1.message_type_code AS message_type_code,
                   a1.message_event_type_code AS message_event_type_code,
                   a1.message_flag_code AS message_flag_code,
                   a1.message_origin_requested_date_time,
                   a1.message_signed_observation_date_time,
                   a1.message_ingestion_date_time,
                   a1.message_created_date_time,
                   a1.document_type_id_text,
                   a1.document_type_text,
                   a1.document_type_coding_system_code,
                   a1.first_insert_date_time,
                   a1.icd_oncology_type_code AS icd_oncology_type_code,
                   a1.predicted_primary_icd_oncology_code AS predicted_primary_icd_oncology_code,
                   a1.predicted_primary_icd_site_desc,
                   a1.suggested_primary_icd_oncology_code AS suggested_primary_icd_oncology_code,
                   a1.suggested_primary_icd_site_desc,
                   a1.submitted_primary_icd_oncology_code AS submitted_primary_icd_oncology_code,
                   a1.submitted_primary_icd_site_desc,
                   a1.transition_of_care_num,
                   a1.user_action_desc,
                   a1.user_action_criticality_text,
                   a1.user_3_4_id AS user_3_4_id,
                   a1.report_assigned_date_time,
                   a1.attending_physician_full_name,
                   a1.pcp_full_name,
                   a1.pcp_phone_num,
                   a1.facility_menmonic_cs AS facility_menmonic_cs,
                   a1.network_mnemonic_cs,
                   a1.model_predicted_sarcoma_ind AS model_predicted_sarcoma_ind,
                   a1.submitted_sarcoma_ind AS submitted_sarcoma_ind,
                   a1.suggested_sarcoma_ind AS suggested_sarcoma_ind,
                   a1.benign_brain_tumor_type_ind AS benign_brain_tumor_type_ind,
                   a1.met_to_brain_tumor_type_ind AS met_to_brain_tumor_type_ind,
                   a1.other_tumor_type_ind AS other_tumor_type_ind,
                   a1.source_system_code AS source_system_code,
                   a1.dw_last_update_date_time
   FROM
     (SELECT DISTINCT cpd.cancer_patient_driver_sk,
                      a.cancer_patient_id_output_sk,
                      a.message_control_id_text,
                      a.user_action_date_time,
                      a.coid,
                      a.company_code,
                      a.patient_dw_id,
                      a.pat_acct_num,
                      a.medical_record_num,
                      a.patient_market_urn,
                      a.last_name,
                      a.first_name,
                      a.birth_date,
                      a.gender_code,
                      a.social_security_num,
                      a.address_line_1_text,
                      a.address_line_2_text,
                      a.city_name,
                      a.state_code,
                      a.zip_code,
                      a.patient_type_code,
                      a.message_type_code,
                      a.message_event_type_code,
                      a.message_flag_code,
                      a.message_origin_requested_date_time,
                      a.message_signed_observation_date_time,
                      a.message_ingestion_date_time,
                      a.message_created_date_time,
                      a.document_type_id_text,
                      a.document_type_text,
                      a.document_type_coding_system_code,
                      a.first_insert_date_time,
                      a.icd_oncology_type_code,
                      a.predicted_primary_icd_oncology_code,
                      a.predicted_primary_icd_site_desc,
                      a.suggested_primary_icd_oncology_code,
                      a.suggested_primary_icd_site_desc,
                      a.submitted_primary_icd_oncology_code,
                      a.submitted_primary_icd_site_desc,
                      a.transition_of_care_num,
                      a.user_action_desc,
                      a.user_action_criticality_text,
                      a.user_3_4_id,
                      a.report_assigned_date_time,
                      a.attending_physician_full_name,
                      a.pcp_full_name,
                      a.pcp_phone_num,
                      a.facility_menmonic_cs,
                      a.network_mnemonic_cs,
                      a.model_predicted_sarcoma_ind,
                      a.submitted_sarcoma_ind,
                      a.suggested_sarcoma_ind,
                      a.benign_brain_tumor_type_ind,
                      a.met_to_brain_tumor_type_ind,
                      a.other_tumor_type_ind,
                      a.source_system_code,
                      a.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS a
      INNER JOIN
        (SELECT cancer_patient_driver.cancer_patient_driver_sk,
                cancer_patient_driver.coid,
                cancer_patient_driver.cp_patient_id,
                cancer_patient_driver.network_mnemonic_cs
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY cancer_patient_driver.cp_patient_id,
                                                                                                                   cancer_patient_driver.network_mnemonic_cs
                                                                                                      ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpd ON a.patient_dw_id = cpd.cp_patient_id
      AND rtrim(a.network_mnemonic_cs) = rtrim(cpd.network_mnemonic_cs)
      UNION DISTINCT SELECT DISTINCT cpd.cancer_patient_driver_sk,
                                     a.cancer_patient_id_output_sk,
                                     a.message_control_id_text,
                                     a.user_action_date_time,
                                     coalesce(a.coid, df2.coid) AS coid,
                                     a.company_code,
                                     a.patient_dw_id,
                                     a.pat_acct_num,
                                     a.medical_record_num,
                                     a.patient_market_urn,
                                     a.last_name,
                                     a.first_name,
                                     a.birth_date,
                                     a.gender_code,
                                     a.social_security_num,
                                     a.address_line_1_text,
                                     a.address_line_2_text,
                                     a.city_name,
                                     a.state_code,
                                     a.zip_code,
                                     a.patient_type_code,
                                     a.message_type_code,
                                     a.message_event_type_code,
                                     a.message_flag_code,
                                     a.message_origin_requested_date_time,
                                     a.message_signed_observation_date_time,
                                     a.message_ingestion_date_time,
                                     a.message_created_date_time,
                                     a.document_type_id_text,
                                     a.document_type_text,
                                     a.document_type_coding_system_code,
                                     a.first_insert_date_time,
                                     a.icd_oncology_type_code,
                                     a.predicted_primary_icd_oncology_code,
                                     a.predicted_primary_icd_site_desc,
                                     a.suggested_primary_icd_oncology_code,
                                     a.suggested_primary_icd_site_desc,
                                     a.submitted_primary_icd_oncology_code,
                                     a.submitted_primary_icd_site_desc,
                                     a.transition_of_care_num,
                                     a.user_action_desc,
                                     a.user_action_criticality_text,
                                     a.user_3_4_id,
                                     a.report_assigned_date_time,
                                     a.attending_physician_full_name,
                                     a.pcp_full_name,
                                     a.pcp_phone_num,
                                     a.facility_menmonic_cs,
                                     a.network_mnemonic_cs,
                                     a.model_predicted_sarcoma_ind,
                                     a.submitted_sarcoma_ind,
                                     a.suggested_sarcoma_ind,
                                     a.benign_brain_tumor_type_ind,
                                     a.met_to_brain_tumor_type_ind,
                                     a.other_tumor_type_ind,
                                     a.source_system_code,
                                     a.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS a
      LEFT OUTER JOIN
        (SELECT clinical_facility.facility_mnemonic_cs,
                clinical_facility.coid,
                clinical_facility.facility_active_ind
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY clinical_facility.facility_mnemonic_cs
                                                                                                  ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS df2 ON rtrim(a.facility_menmonic_cs) = rtrim(df2.facility_mnemonic_cs)
      LEFT OUTER JOIN
        (SELECT cancer_patient_driver.cancer_patient_driver_sk,
                cancer_patient_driver.coid,
                coalesce(concat(cancer_patient_driver.patient_market_urn_text, cancer_patient_driver.network_mnemonic_cs), concat(cancer_patient_driver.medical_record_num, cancer_patient_driver.coid)) AS pid,
                cancer_patient_driver.network_mnemonic_cs
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver QUALIFY row_number() OVER (PARTITION BY pid
                                                                                                      ORDER BY cancer_patient_driver.cancer_patient_driver_sk) = 1) AS cpd ON rtrim(coalesce(concat(a.patient_market_urn, a.network_mnemonic_cs), concat(a.medical_record_num, coalesce(a.coid, df2.coid)))) = rtrim(cpd.pid)
      AND rtrim(coalesce(a.network_mnemonic_cs, '-1')) = rtrim(coalesce(cpd.network_mnemonic_cs, '-1'))
      WHERE (coalesce(a.patient_dw_id, NUMERIC '0'),
             a.network_mnemonic_cs) NOT IN
          (SELECT AS STRUCT coalesce(a_0.patient_dw_id, NUMERIC '0'),
                            a_0.network_mnemonic_cs
           FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output AS a_0
           INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_driver AS cpd_0 ON a_0.patient_dw_id = cpd_0.cp_patient_id
           AND rtrim(a_0.network_mnemonic_cs) = rtrim(cpd_0.network_mnemonic_cs)) ) AS a1) AS ms ON mt.cancer_patient_id_output_driver_sk = ms.cancer_patient_id_output_driver_sk
AND (coalesce(mt.cancer_patient_driver_sk, NUMERIC '0') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '0')
     AND coalesce(mt.cancer_patient_driver_sk, NUMERIC '1') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '1'))
AND (coalesce(mt.cancer_patient_id_output_sk, 0) = coalesce(ms.cancer_patient_id_output_sk, 0)
     AND coalesce(mt.cancer_patient_id_output_sk, 1) = coalesce(ms.cancer_patient_id_output_sk, 1))
AND (upper(coalesce(mt.message_control_id_text, '0')) = upper(coalesce(ms.message_control_id_text, '0'))
     AND upper(coalesce(mt.message_control_id_text, '1')) = upper(coalesce(ms.message_control_id_text, '1')))
AND (coalesce(mt.user_action_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.user_action_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.user_action_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.user_action_date_time, DATETIME '1970-01-01 00:00:01'))
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
AND (upper(coalesce(mt.user_action_criticality_text, '0')) = upper(coalesce(ms.user_action_criticality_text, '0'))
     AND upper(coalesce(mt.user_action_criticality_text, '1')) = upper(coalesce(ms.user_action_criticality_text, '1')))
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
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_id_output_driver_sk,
        cancer_patient_driver_sk,
        cancer_patient_id_output_sk,
        message_control_id_text,
        user_action_date_time,
        coid,
        company_code,
        patient_dw_id,
        pat_acct_num,
        medical_record_num,
        patient_market_urn,
        last_name,
        first_name,
        birth_date,
        gender_code,
        social_security_num,
        address_line_1_text,
        address_line_2_text,
        city_name,
        state_code,
        zip_code,
        patient_type_code,
        message_type_code,
        message_event_type_code,
        message_flag_code,
        message_origin_requested_date_time,
        message_signed_observation_date_time,
        message_ingestion_date_time,
        message_created_date_time,
        document_type_id_text,
        document_type_text,
        document_type_coding_system_code,
        first_insert_date_time,
        icd_oncology_type_code,
        predicted_primary_icd_oncology_code,
        predicted_primary_icd_site_desc,
        suggested_primary_icd_oncology_code,
        suggested_primary_icd_site_desc,
        submitted_primary_icd_oncology_code,
        submitted_primary_icd_site_desc,
        transition_of_care_num,
        user_action_desc,
        user_action_criticality_text,
        user_3_4_id,
        report_assigned_date_time,
        attending_physician_full_name,
        pcp_full_name,
        pcp_phone_num,
        facility_menmonic_cs,
        network_mnemonic_cs,
        model_predicted_sarcoma_ind,
        submitted_sarcoma_ind,
        suggested_sarcoma_ind,
        benign_brain_tumor_type_ind,
        met_to_brain_tumor_type_ind,
        other_tumor_type_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_id_output_driver_sk, ms.cancer_patient_driver_sk, ms.cancer_patient_id_output_sk, ms.message_control_id_text, ms.user_action_date_time, ms.coid, ms.company_code, ms.patient_dw_id, ms.pat_acct_num, ms.medical_record_num, ms.patient_market_urn, ms.last_name, ms.first_name, ms.birth_date, ms.gender_code, ms.social_security_num, ms.address_line_1_text, ms.address_line_2_text, ms.city_name, ms.state_code, ms.zip_code, ms.patient_type_code, ms.message_type_code, ms.message_event_type_code, ms.message_flag_code, ms.message_origin_requested_date_time, ms.message_signed_observation_date_time, ms.message_ingestion_date_time, ms.message_created_date_time, ms.document_type_id_text, ms.document_type_text, ms.document_type_coding_system_code, ms.first_insert_date_time, ms.icd_oncology_type_code, ms.predicted_primary_icd_oncology_code, ms.predicted_primary_icd_site_desc, ms.suggested_primary_icd_oncology_code, ms.suggested_primary_icd_site_desc, ms.submitted_primary_icd_oncology_code, ms.submitted_primary_icd_site_desc, ms.transition_of_care_num, ms.user_action_desc, ms.user_action_criticality_text, ms.user_3_4_id, ms.report_assigned_date_time, ms.attending_physician_full_name, ms.pcp_full_name, ms.pcp_phone_num, ms.facility_menmonic_cs, ms.network_mnemonic_cs, ms.model_predicted_sarcoma_ind, ms.submitted_sarcoma_ind, ms.suggested_sarcoma_ind, ms.benign_brain_tumor_type_ind, ms.met_to_brain_tumor_type_ind, ms.other_tumor_type_ind, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_id_output_driver_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_output_driver
      GROUP BY cancer_patient_id_output_driver_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cancer_patient_id_output_driver');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CANCER_PATIENT_ID_OUTPUT_DRIVER');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF