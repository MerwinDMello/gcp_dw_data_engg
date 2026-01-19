-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/navque_history.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.NavQue_History	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.NavQue_History_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_NavQue_History;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','NavQue_History_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.navque_history;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- where Hashbite_SSK  not in (Select Hashbite_SSK from edwcr_staging.NavQue_History_Stg);
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Insert the new records into the Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.navque_history AS mt USING (
    SELECT DISTINCT
        stg.navque_history_id,
        stg.navque_action_id,
        stg.navque_reason_id,
        stg.tumor_type_id,
        stg.navigator_id,
        stg.coid AS coid,
        stg.company_code AS company_code,
        stg.message_control_id_text,
        stg.message_date,
        stg.navque_insert_date,
        stg.navque_action_date,
        stg.medical_record_num AS medical_record_num,
        stg.patient_market_urn,
        substr(stg.network_mnemonic_cs, 1, 10) AS network_mnemonic_cs,
        stg.transition_of_care_score_num,
        stg.navigated_patient_ind AS navigated_patient_ind,
        stg.message_source_flag AS message_source_flag,
        substr(stg.hashbite_ssk, 1, 60) AS hashbite_ssk,
        stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.navque_history_stg AS stg
      WHERE upper(trim(stg.hashbite_ssk)) NOT IN(
        SELECT
            upper(trim(navque_history.hashbite_ssk))
          FROM
            `hca-hin-dev-cur-ops`.edwcr.navque_history
      )
  ) AS ms
  ON mt.navque_history_id = ms.navque_history_id
   AND mt.navque_action_id = ms.navque_action_id
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
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (navque_history_id, navque_action_id, navque_reason_id, tumor_type_id, navigator_id, coid, company_code, message_control_id_text, message_date, navque_insert_date, navque_action_date, medical_record_num, patient_market_urn, network_mnemonic_cs, transition_of_care_score_num, navigated_patient_ind, message_source_flag, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.navque_history_id, ms.navque_action_id, ms.navque_reason_id, ms.tumor_type_id, ms.navigator_id, ms.coid, ms.company_code, ms.message_control_id_text, ms.message_date, ms.navque_insert_date, ms.navque_action_date, ms.medical_record_num, ms.patient_market_urn, ms.network_mnemonic_cs, ms.transition_of_care_score_num, ms.navigated_patient_ind, ms.message_source_flag, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','NavQue_History');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
