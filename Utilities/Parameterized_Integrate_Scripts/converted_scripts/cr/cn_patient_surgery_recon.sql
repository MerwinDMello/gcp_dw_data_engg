-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_surgery_recon.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Surgery_Reconstruction	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_Patient_Surgery_Reconstruction_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_SURGERY_RECON;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Surgery_Reconstruction_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_surgery_reconstruction WHERE upper(cn_patient_surgery_reconstruction.hashbite_ssk) NOT IN(
    SELECT
        upper(cn_patient_surgery_reconstruction_stg.hashbite_ssk) AS hashbite_ssk
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_surgery_reconstruction_stg
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
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_surgery_reconstruction AS mt USING (
    SELECT DISTINCT
        stg.cn_patient_surgery_recstr_sid,
        stg.core_record_type_id,
        rs.side_id,
        stg.med_spcl_physician_id,
        stg.nav_patient_id,
        stg.tumor_type_id,
        stg.diagnosis_result_id,
        stg.nav_diagnosis_id,
        stg.navigator_id,
        stg.coid AS coid,
        stg.company_code AS company_code,
        stg.recstr_date,
        stg.surgery_recstr_type_text,
        stg.declined_ind AS declined_ind,
        stg.hashbite_ssk,
        stg.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_surgery_reconstruction_stg AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_side AS rs ON upper(trim(stg.reconsurgeryside)) = upper(trim(rs.side_desc))
      WHERE upper(stg.hashbite_ssk) NOT IN(
        SELECT
            upper(cn_patient_surgery_reconstruction.hashbite_ssk) AS hashbite_ssk
          FROM
            `hca-hin-dev-cur-ops`.edwcr.cn_patient_surgery_reconstruction
      )
  ) AS ms
  ON mt.cn_patient_surgery_recstr_sid = ms.cn_patient_surgery_recstr_sid
   AND (coalesce(mt.core_record_type_id, 0) = coalesce(ms.core_record_type_id, 0)
   AND coalesce(mt.core_record_type_id, 1) = coalesce(ms.core_record_type_id, 1))
   AND (coalesce(mt.surgery_recstr_side_id, 0) = coalesce(ms.side_id, 0)
   AND coalesce(mt.surgery_recstr_side_id, 1) = coalesce(ms.side_id, 1))
   AND (coalesce(mt.med_spcl_physician_id, 0) = coalesce(ms.med_spcl_physician_id, 0)
   AND coalesce(mt.med_spcl_physician_id, 1) = coalesce(ms.med_spcl_physician_id, 1))
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
   AND (coalesce(mt.recstr_date, DATE '1970-01-01') = coalesce(ms.recstr_date, DATE '1970-01-01')
   AND coalesce(mt.recstr_date, DATE '1970-01-02') = coalesce(ms.recstr_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.surgery_recstr_type_text, '0')) = upper(coalesce(ms.surgery_recstr_type_text, '0'))
   AND upper(coalesce(mt.surgery_recstr_type_text, '1')) = upper(coalesce(ms.surgery_recstr_type_text, '1')))
   AND (upper(coalesce(mt.declined_ind, '0')) = upper(coalesce(ms.declined_ind, '0'))
   AND upper(coalesce(mt.declined_ind, '1')) = upper(coalesce(ms.declined_ind, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_surgery_recstr_sid, core_record_type_id, surgery_recstr_side_id, med_spcl_physician_id, nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, coid, company_code, recstr_date, surgery_recstr_type_text, declined_ind, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_surgery_recstr_sid, ms.core_record_type_id, ms.side_id, ms.med_spcl_physician_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.recstr_date, ms.surgery_recstr_type_text, ms.declined_ind, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Surgery_Reconstruction');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
