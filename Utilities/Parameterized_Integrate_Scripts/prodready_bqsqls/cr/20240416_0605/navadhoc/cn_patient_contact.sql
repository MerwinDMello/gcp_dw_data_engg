DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_contact.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_Patient_Contact		                ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_staging.CN_Patient_Contact_stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Contact;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Contact_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_contact
WHERE upper(cn_patient_contact.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_contact_stg.hashbite_ssk) AS hashbite_ssk
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_contact_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_contact AS mt USING
  (SELECT DISTINCT cpstg.cn_patient_contact_sid,
                   rcp.contact_purpose_id AS contact_purpose_id,
                   rcm.contact_method_id,
                   cpe.contact_person_id AS contact_person_id,
                   cpstg.nav_patient_id,
                   cpstg.tumor_type_id,
                   cpstg.diagnosis_result_id,
                   cpstg.nav_diagnosis_id,
                   cpstg.navigator_id,
                   cpstg.coid AS coid,
                   'H' AS company_code,
                   cpstg.contact_date,
                   cpstg.other_purpose_detail_text,
                   cpstg.other_person_contacted_text,
                   cpstg.time_spent_amount_text,
                   cpstg.comment_text,
                   cpstg.hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_contact_stg AS cpstg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_contact_purpose AS rcp ON upper(rtrim(cpstg.purpose_of_contact)) = upper(rtrim(rcp.contact_purpose_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_contact_person AS cpe ON upper(rtrim(cpstg.person_of_contact)) = upper(rtrim(cpe.contact_person_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_contact_method AS rcm ON upper(rtrim(cpstg.method_of_contact)) = upper(rtrim(rcm.contact_method_desc))
   WHERE upper(cpstg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_contact.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_contact) ) AS ms ON mt.cn_patient_contact_sid = ms.cn_patient_contact_sid
AND (coalesce(mt.contact_purpose_id, 0) = coalesce(ms.contact_purpose_id, 0)
     AND coalesce(mt.contact_purpose_id, 1) = coalesce(ms.contact_purpose_id, 1))
AND (coalesce(mt.contact_method_id, 0) = coalesce(ms.contact_method_id, 0)
     AND coalesce(mt.contact_method_id, 1) = coalesce(ms.contact_method_id, 1))
AND (coalesce(mt.contact_person_id, 0) = coalesce(ms.contact_person_id, 0)
     AND coalesce(mt.contact_person_id, 1) = coalesce(ms.contact_person_id, 1))
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
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (coalesce(mt.contact_date, DATE '1970-01-01') = coalesce(ms.contact_date, DATE '1970-01-01')
     AND coalesce(mt.contact_date, DATE '1970-01-02') = coalesce(ms.contact_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.other_purpose_detail_text, '0')) = upper(coalesce(ms.other_purpose_detail_text, '0'))
     AND upper(coalesce(mt.other_purpose_detail_text, '1')) = upper(coalesce(ms.other_purpose_detail_text, '1')))
AND (upper(coalesce(mt.other_person_contacted_text, '0')) = upper(coalesce(ms.other_person_contacted_text, '0'))
     AND upper(coalesce(mt.other_person_contacted_text, '1')) = upper(coalesce(ms.other_person_contacted_text, '1')))
AND (upper(coalesce(mt.time_spent_amount_text, '0')) = upper(coalesce(ms.time_spent_amount_text, '0'))
     AND upper(coalesce(mt.time_spent_amount_text, '1')) = upper(coalesce(ms.time_spent_amount_text, '1')))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_contact_sid,
        contact_purpose_id,
        contact_method_id,
        contact_person_id,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        contact_date,
        other_purpose_detail_text,
        other_person_contacted_text,
        time_spent_amount_text,
        comment_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_contact_sid, ms.contact_purpose_id, ms.contact_method_id, ms.contact_person_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.contact_date, ms.other_purpose_detail_text, ms.other_person_contacted_text, ms.time_spent_amount_text, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_contact_sid
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_contact
      GROUP BY cn_patient_contact_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_contact');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Contact');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF