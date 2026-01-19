DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/navque_patient_tumor_driver.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.NAVQUE_PATIENT_TUMOR_DRIVER                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		  		   : EDWCR.NavQue_History     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_NAVQUE_PATIENT_TUMOR_DRIVER;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','NavQue_History');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.navque_patient_tumor_driver;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.navque_patient_tumor_driver AS mt USING
  (SELECT DISTINCT CAST(row_number() OVER (
                                           ORDER BY CAST(sk.cancer_patient_driver_sk AS INT64), ctd.cancer_tumor_driver_sk, a.navque_history_id) AS NUMERIC) AS navque_patient_tumor_driver_sk,
                   sk.cancer_patient_driver_sk AS cancer_patient_driver_sk,
                   ctd.cancer_tumor_driver_sk AS cancer_tumor_driver_sk,
                   a.navque_history_id AS navque_history_id,
                   a.navque_action_id AS navque_action_id,
                   a.navque_reason_id AS navque_reason_id,
                   a.tumor_type_id AS tumor_type_id,
                   a.navigator_id AS navigator_id,
                   a.coid AS coid,
                   a.company_code AS company_code,
                   substr(a.message_control_id_text, 1, 50) AS message_control_id_text,
                   a.message_date AS message_date,
                   a.navque_insert_date AS navque_insert_date,
                   a.navque_action_date AS navque_action_date,
                   a.medical_record_num AS medical_record_num,
                   a.patient_market_urn AS patient_market_urn,
                   a.network_mnemonic_cs AS network_mnemonic_cs,
                   a.transition_of_care_score_num AS transition_of_care_score_num,
                   a.navigated_patient_ind AS navigated_patient_ind,
                   a.message_source_flag AS message_source_flag,
                   a.hashbite_ssk AS hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_base_views.navque_history AS a
   LEFT OUTER JOIN
     (SELECT cpio.*
      FROM
        (SELECT DISTINCT a_0.cancer_patient_driver_sk,
                         a_0.message_control_id_text,
                         a_0.user_action_date_time
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output_driver AS a_0) AS cpio QUALIFY row_number() OVER (PARTITION BY upper(cpio.message_control_id_text)
                                                                                                                                ORDER BY cpio.user_action_date_time DESC) = 1) AS sk ON upper(rtrim(a.message_control_id_text)) = upper(rtrim(sk.message_control_id_text))
   LEFT OUTER JOIN
     (SELECT cancer_tumor_driver.cp_icd_oncology_code,
             cancer_tumor_driver.cn_navque_tumor_type_id,
             cancer_tumor_driver.cancer_tumor_driver_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_tumor_driver QUALIFY row_number() OVER (PARTITION BY cancer_tumor_driver.cn_navque_tumor_type_id
                                                                                                 ORDER BY cancer_tumor_driver.cancer_tumor_driver_sk DESC) = 1) AS ctd ON a.tumor_type_id = ctd.cn_navque_tumor_type_id) AS ms ON mt.navque_patient_tumor_driver_sk = ms.navque_patient_tumor_driver_sk
AND (coalesce(mt.cancer_patient_driver_sk, NUMERIC '0') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '0')
     AND coalesce(mt.cancer_patient_driver_sk, NUMERIC '1') = coalesce(ms.cancer_patient_driver_sk, NUMERIC '1'))
AND (coalesce(mt.cancer_tumor_driver_sk, 0) = coalesce(ms.cancer_tumor_driver_sk, 0)
     AND coalesce(mt.cancer_tumor_driver_sk, 1) = coalesce(ms.cancer_tumor_driver_sk, 1))
AND (coalesce(mt.navque_history_id, 0) = coalesce(ms.navque_history_id, 0)
     AND coalesce(mt.navque_history_id, 1) = coalesce(ms.navque_history_id, 1))
AND (coalesce(mt.navque_action_id, 0) = coalesce(ms.navque_action_id, 0)
     AND coalesce(mt.navque_action_id, 1) = coalesce(ms.navque_action_id, 1))
AND (coalesce(mt.navque_reason_id, 0) = coalesce(ms.navque_reason_id, 0)
     AND coalesce(mt.navque_reason_id, 1) = coalesce(ms.navque_reason_id, 1))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.message_control_id_text, '0')) = upper(coalesce(ms.message_control_id_text, '0'))
     AND upper(coalesce(mt.message_control_id_text, '1')) = upper(coalesce(ms.message_control_id_text, '1')))
AND (coalesce(mt.message_date, DATE '1970-01-01') = coalesce(ms.message_date, DATE '1970-01-01')
     AND coalesce(mt.message_date, DATE '1970-01-02') = coalesce(ms.message_date, DATE '1970-01-02'))
AND (coalesce(mt.navque_insert_date, DATE '1970-01-01') = coalesce(ms.navque_insert_date, DATE '1970-01-01')
     AND coalesce(mt.navque_insert_date, DATE '1970-01-02') = coalesce(ms.navque_insert_date, DATE '1970-01-02'))
AND (coalesce(mt.navque_action_date, DATE '1970-01-01') = coalesce(ms.navque_action_date, DATE '1970-01-01')
     AND coalesce(mt.navque_action_date, DATE '1970-01-02') = coalesce(ms.navque_action_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND (upper(coalesce(mt.patient_market_urn, '0')) = upper(coalesce(ms.patient_market_urn, '0'))
     AND upper(coalesce(mt.patient_market_urn, '1')) = upper(coalesce(ms.patient_market_urn, '1')))
AND (coalesce(mt.network_mnemonic_cs, '0') = coalesce(ms.network_mnemonic_cs, '0')
     AND coalesce(mt.network_mnemonic_cs, '1') = coalesce(ms.network_mnemonic_cs, '1'))
AND (coalesce(mt.transition_of_care_score_num, 0) = coalesce(ms.transition_of_care_score_num, 0)
     AND coalesce(mt.transition_of_care_score_num, 1) = coalesce(ms.transition_of_care_score_num, 1))
AND (upper(coalesce(mt.navigated_patient_ind, '0')) = upper(coalesce(ms.navigated_patient_ind, '0'))
     AND upper(coalesce(mt.navigated_patient_ind, '1')) = upper(coalesce(ms.navigated_patient_ind, '1')))
AND (upper(coalesce(mt.message_source_flag, '0')) = upper(coalesce(ms.message_source_flag, '0'))
     AND upper(coalesce(mt.message_source_flag, '1')) = upper(coalesce(ms.message_source_flag, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (navque_patient_tumor_driver_sk,
        cancer_patient_driver_sk,
        cancer_tumor_driver_sk,
        navque_history_id,
        navque_action_id,
        navque_reason_id,
        tumor_type_id,
        navigator_id,
        coid,
        company_code,
        message_control_id_text,
        message_date,
        navque_insert_date,
        navque_action_date,
        medical_record_num,
        patient_market_urn,
        network_mnemonic_cs,
        transition_of_care_score_num,
        navigated_patient_ind,
        message_source_flag,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.navque_patient_tumor_driver_sk, ms.cancer_patient_driver_sk, ms.cancer_tumor_driver_sk, ms.navque_history_id, ms.navque_action_id, ms.navque_reason_id, ms.tumor_type_id, ms.navigator_id, ms.coid, ms.company_code, ms.message_control_id_text, ms.message_date, ms.navque_insert_date, ms.navque_action_date, ms.medical_record_num, ms.patient_market_urn, ms.network_mnemonic_cs, ms.transition_of_care_score_num, ms.navigated_patient_ind, ms.message_source_flag, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT navque_patient_tumor_driver_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.navque_patient_tumor_driver
      GROUP BY navque_patient_tumor_driver_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.navque_patient_tumor_driver');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','NAVQUE_PATIENT_TUMOR_DRIVER');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;