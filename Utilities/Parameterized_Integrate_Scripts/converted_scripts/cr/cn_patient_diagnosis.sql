-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_diagnosis.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Diagnosis	           		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_Patient_Diagnosis_STG			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_DIAGNOSIS;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Diagnosis_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_diagnosis WHERE upper(cn_patient_diagnosis.hashbite_ssk) NOT IN(
    SELECT
        upper(cn_patient_diagnosis_stg.hashbite_ssk) AS hashbite_ssk
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_diagnosis_stg
  );
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Insert the new records into the Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_diagnosis AS mt USING (
    SELECT DISTINCT
        stg.cn_patient_diagnosis_sid,
        stg.nav_patient_id,
        stg.tumor_type_id,
        stg.diagnosis_result_id,
        stg.nav_diagnosis_id,
        stg.navigator_id,
        stg.coid AS coid,
        'H' AS company_code,
        stg.general_diagnosis_name,
        stg.diagnosis_date,
        rdd.diagnosis_detail_id AS diagnosis_detail_id,
        rs.side_id AS diagnosis_side_id,
        stg.hashbite_ssk,
        'N' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_diagnosis_stg AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_side AS rs ON upper(rtrim(stg.diagnosisside)) = upper(rtrim(rs.side_desc))
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_diagnosis_detail AS rdd ON upper(rtrim(coalesce(trim(stg.diagnosismetastatic), 'X'))) = upper(rtrim(coalesce(trim(rdd.diagnosis_detail_desc), 'X')))
         AND upper(rtrim(coalesce(trim(stg.diagnosisindicator), 'XX'))) = upper(rtrim(coalesce(trim(rdd.diagnosis_indicator_text), 'XX')))
      WHERE upper(stg.hashbite_ssk) NOT IN(
        SELECT
            upper(cn_patient_diagnosis.hashbite_ssk) AS hashbite_ssk
          FROM
            `hca-hin-dev-cur-ops`.edwcr.cn_patient_diagnosis
      )
  ) AS ms
  ON mt.cn_patient_diagnosis_sid = ms.cn_patient_diagnosis_sid
   AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
   AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
   AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
   AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
   AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
   AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
   AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
   AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
   AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
   AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
   AND mt.coid = ms.coid
   AND mt.company_code = ms.company_code
   AND (upper(coalesce(mt.general_diagnosis_name, '0')) = upper(coalesce(ms.general_diagnosis_name, '0'))
   AND upper(coalesce(mt.general_diagnosis_name, '1')) = upper(coalesce(ms.general_diagnosis_name, '1')))
   AND (coalesce(mt.diagnosis_date, DATE '1970-01-01') = coalesce(ms.diagnosis_date, DATE '1970-01-01')
   AND coalesce(mt.diagnosis_date, DATE '1970-01-02') = coalesce(ms.diagnosis_date, DATE '1970-01-02'))
   AND (coalesce(mt.diagnosis_detail_id, 0) = coalesce(ms.diagnosis_detail_id, 0)
   AND coalesce(mt.diagnosis_detail_id, 1) = coalesce(ms.diagnosis_detail_id, 1))
   AND (coalesce(mt.diagnosis_side_id, 0) = coalesce(ms.diagnosis_side_id, 0)
   AND coalesce(mt.diagnosis_side_id, 1) = coalesce(ms.diagnosis_side_id, 1))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_diagnosis_sid, nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, coid, company_code, general_diagnosis_name, diagnosis_date, diagnosis_detail_id, diagnosis_side_id, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_diagnosis_sid, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.general_diagnosis_name, ms.diagnosis_date, ms.diagnosis_detail_id, ms.diagnosis_side_id, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_DIAGNOSIS');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
