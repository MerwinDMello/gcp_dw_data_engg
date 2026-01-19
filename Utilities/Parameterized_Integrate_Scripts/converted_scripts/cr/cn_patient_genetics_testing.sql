-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_genetics_testing.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Genetics_Testing	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_Patient_Genetics_Testing_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_GENETICS_TESTING;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Genetics_Testing_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Delete the records from Core table which don't exist in the Staging table */
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_genetics_testing WHERE upper(cn_patient_genetics_testing.hashbite_ssk) NOT IN(
    SELECT
        upper(cn_patient_genetics_testing_stg.hbsource) AS hbsource
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_genetics_testing_stg
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
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_genetics_testing AS mt USING (
    SELECT DISTINCT
        pgt.geneticstestingfactid,
        pgt.corerecordid,
        CAST(pgt.patientdimid as NUMERIC) AS nav_patient_id,
        pgt.tumortypedimid,
        pgt.diagnosisresultid,
        pgt.diagnosisdimid,
        pgt.coid AS coid,
        'H' AS company_code,
        pgt.navigatordimid,
        pgt.geneticsdate,
        substr(trim(pgt.geneticstesttype), 1, 100) AS test_name,
        substr(trim(pgt.geneticsspecialist), 1, 50) AS genetics_specialist_name,
        brca.breast_cancer_type_id,
        substr(trim(pgt.geneticscomments), 1, 1000) AS comment_text,
        pgt.hbsource,
        'N' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_genetics_testing_stg AS pgt
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_breast_cancer_type AS brca ON upper(rtrim(pgt.geneticsbrcatype)) = upper(rtrim(brca.breast_cancer_type_desc))
      WHERE upper(pgt.hbsource) NOT IN(
        SELECT
            upper(cn_patient_genetics_testing.hashbite_ssk) AS hashbite_ssk
          FROM
            `hca-hin-dev-cur-ops`.edwcr.cn_patient_genetics_testing
      )
  ) AS ms
  ON mt.cn_patient_genetics_testing_sid = ms.geneticstestingfactid
   AND (coalesce(mt.core_record_type_id, 0) = coalesce(ms.corerecordid, 0)
   AND coalesce(mt.core_record_type_id, 1) = coalesce(ms.corerecordid, 1))
   AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
   AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
   AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumortypedimid, 0)
   AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumortypedimid, 1))
   AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosisresultid, 0)
   AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosisresultid, 1))
   AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.diagnosisdimid, 0)
   AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.diagnosisdimid, 1))
   AND mt.coid = ms.coid
   AND mt.company_code = ms.company_code
   AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigatordimid, 0)
   AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigatordimid, 1))
   AND (coalesce(mt.testing_date, DATE '1970-01-01') = coalesce(ms.geneticsdate, DATE '1970-01-01')
   AND coalesce(mt.testing_date, DATE '1970-01-02') = coalesce(ms.geneticsdate, DATE '1970-01-02'))
   AND (upper(coalesce(mt.test_name, '0')) = upper(coalesce(ms.test_name, '0'))
   AND upper(coalesce(mt.test_name, '1')) = upper(coalesce(ms.test_name, '1')))
   AND (upper(coalesce(mt.genetics_specialist_name, '0')) = upper(coalesce(ms.genetics_specialist_name, '0'))
   AND upper(coalesce(mt.genetics_specialist_name, '1')) = upper(coalesce(ms.genetics_specialist_name, '1')))
   AND (coalesce(mt.breast_cancer_type_id, 0) = coalesce(ms.breast_cancer_type_id, 0)
   AND coalesce(mt.breast_cancer_type_id, 1) = coalesce(ms.breast_cancer_type_id, 1))
   AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
   AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hbsource, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hbsource, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_genetics_testing_sid, core_record_type_id, nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, coid, company_code, navigator_id, testing_date, test_name, genetics_specialist_name, breast_cancer_type_id, comment_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.geneticstestingfactid, ms.corerecordid, ms.nav_patient_id, ms.tumortypedimid, ms.diagnosisresultid, ms.diagnosisdimid, ms.coid, ms.company_code, ms.navigatordimid, ms.geneticsdate, ms.test_name, ms.genetics_specialist_name, ms.breast_cancer_type_id, ms.comment_text, ms.hbsource, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_GENETICS_TESTING');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
