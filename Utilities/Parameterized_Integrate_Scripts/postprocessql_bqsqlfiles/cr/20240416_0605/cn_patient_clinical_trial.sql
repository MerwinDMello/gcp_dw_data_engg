DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_clinical_trial.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_Patient_Clinical_Trial	                ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_STAGING.CN_Patient_Clinical_Trial_Stg	##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Clinical_Trial;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Clinical_Trial_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_clinical_trial
WHERE upper(cn_patient_clinical_trial.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_clinical_trial_stg.hashbite_ssk) AS hashbite_ssk
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_clinical_trial_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_clinical_trial AS mt USING
  (SELECT DISTINCT /*,Clinical_Trial_Evaluated_Ind
        ,Clinical_Trial_Evaluated_Date
        ,Clinical_Trial_Not_Offered_Text
        ,Clinical_Trial_Not_Offered_Other_Text
        ,Clinical_Trial_Other_Name
        ,Not_Screened_Reason_Text
        ,Not_Screened_Other_Reason_Text*/ cn_patient_clinical_trial_stg.cn_patient_clinical_trial_sid,
                                          cn_patient_clinical_trial_stg.nav_patient_id,
                                          cn_patient_clinical_trial_stg.tumor_type_id,
                                          cn_patient_clinical_trial_stg.diagnosis_result_id,
                                          cn_patient_clinical_trial_stg.nav_diagnosis_id,
                                          cn_patient_clinical_trial_stg.navigator_id,
                                          cn_patient_clinical_trial_stg.coid AS coid,
                                          cn_patient_clinical_trial_stg.company_code AS company_code,
                                          cn_patient_clinical_trial_stg.clinical_trial_name,
                                          cn_patient_clinical_trial_stg.clinical_trial_enrolled_ind AS clinical_trial_enrolled_ind,
                                          cn_patient_clinical_trial_stg.clinical_trial_enrolled_date,
                                          cn_patient_clinical_trial_stg.clinical_trial_offered_ind AS clinical_trial_offered_ind,
                                          cn_patient_clinical_trial_stg.clinical_trial_offered_date, /*,case when Clinical_Trial_Evaluated_Ind='True' then 'Y'
          when Clinical_Trial_Evaluated_Ind='False' then 'N'
          else null
          end as Clinical_Trial_Evaluated_Ind
        ,Clinical_Trial_Evaluated_Date
        ,Clinical_Trial_Not_Off_Text
        ,Clinical_Trial_Not_Off_Ot_Text
        ,Clinical_Trial_Other_Name
        ,Not_Screened_Reason_Text
        ,Not_Screened_Other_Reason_Text */ cn_patient_clinical_trial_stg.hashbite_ssk,
                                           cn_patient_clinical_trial_stg.source_system_code AS source_system_code,
                                           datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_clinical_trial_stg
   WHERE upper(cn_patient_clinical_trial_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_clinical_trial.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_clinical_trial) ) AS ms ON mt.cn_patient_clinical_trial_sid = ms.cn_patient_clinical_trial_sid
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
AND (upper(coalesce(mt.clinical_trial_name, '0')) = upper(coalesce(ms.clinical_trial_name, '0'))
     AND upper(coalesce(mt.clinical_trial_name, '1')) = upper(coalesce(ms.clinical_trial_name, '1')))
AND (upper(coalesce(mt.clinical_trial_enrolled_ind, '0')) = upper(coalesce(ms.clinical_trial_enrolled_ind, '0'))
     AND upper(coalesce(mt.clinical_trial_enrolled_ind, '1')) = upper(coalesce(ms.clinical_trial_enrolled_ind, '1')))
AND (coalesce(mt.clinical_trial_enrolled_date, DATE '1970-01-01') = coalesce(ms.clinical_trial_enrolled_date, DATE '1970-01-01')
     AND coalesce(mt.clinical_trial_enrolled_date, DATE '1970-01-02') = coalesce(ms.clinical_trial_enrolled_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.clinical_trial_offered_ind, '0')) = upper(coalesce(ms.clinical_trial_offered_ind, '0'))
     AND upper(coalesce(mt.clinical_trial_offered_ind, '1')) = upper(coalesce(ms.clinical_trial_offered_ind, '1')))
AND (coalesce(mt.clinical_trial_offered_date, DATE '1970-01-01') = coalesce(ms.clinical_trial_offered_date, DATE '1970-01-01')
     AND coalesce(mt.clinical_trial_offered_date, DATE '1970-01-02') = coalesce(ms.clinical_trial_offered_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_clinical_trial_sid,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        clinical_trial_name,
        clinical_trial_enrolled_ind,
        clinical_trial_enrolled_date,
        clinical_trial_offered_ind,
        clinical_trial_offered_date,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_clinical_trial_sid, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.clinical_trial_name, ms.clinical_trial_enrolled_ind, ms.clinical_trial_enrolled_date, ms.clinical_trial_offered_ind, ms.clinical_trial_offered_date, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_clinical_trial_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_clinical_trial
      GROUP BY cn_patient_clinical_trial_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_clinical_trial');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Clinical_Trial');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF