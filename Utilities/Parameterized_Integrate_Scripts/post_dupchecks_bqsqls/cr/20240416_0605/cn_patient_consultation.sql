DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_consultation.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PATIENT_CONSULTATION	                ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_staging.CN_Patient_Consultation_stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_CONSULTATION;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Consultation_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_consultation
WHERE upper(cn_patient_consultation.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_consultation_stg.hashbite_ssk) AS hashbite_ssk
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_consultation_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_consultation AS mt USING
  (SELECT DISTINCT cn_patient_consultation_stg.cn_patient_consultation_sid,
                   cn_patient_consultation_stg.tumor_type_id,
                   cn_patient_consultation_stg.diagnosis_result_id,
                   cn_patient_consultation_stg.nav_diagnosis_id,
                   cn_patient_consultation_stg.navigator_id,
                   cn_patient_consultation_stg.consult_type_id,
                   cn_patient_consultation_stg.nav_patient_id,
                   cn_patient_consultation_stg.coid AS coid,
                   cn_patient_consultation_stg.company_code AS company_code,
                   cn_patient_consultation_stg.med_spcl_physician_id,
                   cn_patient_consultation_stg.consult_other_type_text,
                   cn_patient_consultation_stg.consult_date,
                   cn_patient_consultation_stg.consult_phone_num,
                   cn_patient_consultation_stg.consult_notes_text,
                   cn_patient_consultation_stg.hashbite_ssk,
                   cn_patient_consultation_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_consultation_stg
   WHERE upper(cn_patient_consultation_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_consultation.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_consultation) ) AS ms ON mt.cn_patient_consultation_sid = ms.cn_patient_consultation_sid
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
     AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
     AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND (coalesce(mt.consult_type_id, 0) = coalesce(ms.consult_type_id, 0)
     AND coalesce(mt.consult_type_id, 1) = coalesce(ms.consult_type_id, 1))
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.med_spcl_physician_id, 0) = coalesce(ms.med_spcl_physician_id, 0)
     AND coalesce(mt.med_spcl_physician_id, 1) = coalesce(ms.med_spcl_physician_id, 1))
AND (upper(coalesce(mt.consult_other_type_text, '0')) = upper(coalesce(ms.consult_other_type_text, '0'))
     AND upper(coalesce(mt.consult_other_type_text, '1')) = upper(coalesce(ms.consult_other_type_text, '1')))
AND (coalesce(mt.consult_date, DATE '1970-01-01') = coalesce(ms.consult_date, DATE '1970-01-01')
     AND coalesce(mt.consult_date, DATE '1970-01-02') = coalesce(ms.consult_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.consult_phone_num, '0')) = upper(coalesce(ms.consult_phone_num, '0'))
     AND upper(coalesce(mt.consult_phone_num, '1')) = upper(coalesce(ms.consult_phone_num, '1')))
AND (upper(coalesce(mt.consult_notes_text, '0')) = upper(coalesce(ms.consult_notes_text, '0'))
     AND upper(coalesce(mt.consult_notes_text, '1')) = upper(coalesce(ms.consult_notes_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_consultation_sid,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        consult_type_id,
        nav_patient_id,
        coid,
        company_code,
        med_spcl_physician_id,
        consult_other_type_text,
        consult_date,
        consult_phone_num,
        consult_notes_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_consultation_sid, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.consult_type_id, ms.nav_patient_id, ms.coid, ms.company_code, ms.med_spcl_physician_id, ms.consult_other_type_text, ms.consult_date, ms.consult_phone_num, ms.consult_notes_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_consultation_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_consultation
      GROUP BY cn_patient_consultation_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_consultation');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_PATIENT_CONSULTATION');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;