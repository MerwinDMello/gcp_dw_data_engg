-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_history.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_HISTORY		             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_HISTORY_STG			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_PATIENT_HISTORY;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CR_PATIENT_HISTORY_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Truncate Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_history;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Populate Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_history AS mt USING (
    SELECT DISTINCT
        stg.patientid AS cr_patient_id,
        stg.tumorid AS tumor_id,
        coalesce(lkp.master_lookup_sid, (
          SELECT
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'SMOKE HISTORY'
             AND upper(rtrim(rlc.lookup_code)) = '-99'
        )) AS smoked_history_id,
        coalesce(lkp1.master_lookup_sid, (
          SELECT
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'FAMILY HISTORY'
             AND upper(rtrim(rlc.lookup_code)) = '-99'
        )) AS family_cancer_history_type_id,
        coalesce(lkp2.master_lookup_sid, (
          SELECT
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'PATIENT HISTORY'
             AND upper(rtrim(rlc.lookup_code)) = '-99'
        )) AS patient_cancer_history_type_id,
        stg.amounttobacco AS tobacco_amount_text,
        stg.yrstobacco1 AS years_tobacco_used_num_text,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_history_stg AS stg
        LEFT OUTER JOIN (
          SELECT
              rln.lookup_sid,
              rlc.lookup_code,
              rlc.lookup_sub_code,
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'SMOKE HISTORY'
        ) AS lkp ON upper(rtrim(stg.smokhist)) = upper(rtrim(lkp.lookup_code))
        LEFT OUTER JOIN (
          SELECT
              rln.lookup_sid,
              rlc.lookup_code,
              rlc.lookup_sub_code,
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'FAMILY HISTORY'
        ) AS lkp1 ON upper(rtrim(stg.famhxca)) = upper(rtrim(lkp1.lookup_code))
        LEFT OUTER JOIN (
          SELECT
              rln.lookup_sid,
              rlc.lookup_code,
              rlc.lookup_sub_code,
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'PATIENT HISTORY'
        ) AS lkp2 ON upper(rtrim(stg.pthxca)) = upper(rtrim(lkp2.lookup_code))
  ) AS ms
  ON mt.cr_patient_id = ms.cr_patient_id
   AND mt.tumor_id = ms.tumor_id
   AND (coalesce(mt.smoked_history_id, 0) = coalesce(ms.smoked_history_id, 0)
   AND coalesce(mt.smoked_history_id, 1) = coalesce(ms.smoked_history_id, 1))
   AND (coalesce(mt.family_cancer_history_type_id, 0) = coalesce(ms.family_cancer_history_type_id, 0)
   AND coalesce(mt.family_cancer_history_type_id, 1) = coalesce(ms.family_cancer_history_type_id, 1))
   AND (coalesce(mt.patient_cancer_history_type_id, 0) = coalesce(ms.patient_cancer_history_type_id, 0)
   AND coalesce(mt.patient_cancer_history_type_id, 1) = coalesce(ms.patient_cancer_history_type_id, 1))
   AND (upper(coalesce(mt.tobacco_amt_text, '0')) = upper(coalesce(ms.tobacco_amount_text, '0'))
   AND upper(coalesce(mt.tobacco_amt_text, '1')) = upper(coalesce(ms.tobacco_amount_text, '1')))
   AND (upper(coalesce(mt.years_tobacco_used_num_text, '0')) = upper(coalesce(ms.years_tobacco_used_num_text, '0'))
   AND upper(coalesce(mt.years_tobacco_used_num_text, '1')) = upper(coalesce(ms.years_tobacco_used_num_text, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cr_patient_id, tumor_id, smoked_history_id, family_cancer_history_type_id, patient_cancer_history_type_id, tobacco_amt_text, years_tobacco_used_num_text, source_system_code, dw_last_update_date_time)
      VALUES (ms.cr_patient_id, ms.tumor_id, ms.smoked_history_id, ms.family_cancer_history_type_id, ms.patient_cancer_history_type_id, ms.tobacco_amount_text, ms.years_tobacco_used_num_text, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CR_PATIENT_HISTORY');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
