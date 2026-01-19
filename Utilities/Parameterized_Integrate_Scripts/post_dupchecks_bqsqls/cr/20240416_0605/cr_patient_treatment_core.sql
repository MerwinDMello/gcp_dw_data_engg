DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_treatment_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #########################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_CONTACT             		#
-- #  TARGET  DATABASE	   	: EDWCR	 				#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_CONTACT_WRK	#
-- #	                                                                #
-- #  INITIAL RELEASE	   	: 					#
-- #  PROJECT             	: 	 		    			#
-- #  ---------------------------------------------------------------------#
-- #                                                                       #
-- #########################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_CONTACT;;
 --' FOR SESSION;;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_treatment;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_treatment AS mt USING
  (SELECT DISTINCT cr_patient_treatment_wrk.treatment_id,
                   cr_patient_treatment_wrk.tumor_id,
                   cr_patient_treatment_wrk.treatment_hospital_id,
                   cr_patient_treatment_wrk.treatment_type_id,
                   cr_patient_treatment_wrk.surgical_site_id,
                   cr_patient_treatment_wrk.surgical_margin_result_id,
                   cr_patient_treatment_wrk.treatment_type_group_id,
                   cr_patient_treatment_wrk.clinical_trial_start_date,
                   cr_patient_treatment_wrk.treatment_start_date,
                   cr_patient_treatment_wrk.clinical_trial_text,
                   cr_patient_treatment_wrk.comment_text,
                   cr_patient_treatment_wrk.treatment_performing_physician_code AS treatment_performing_physician_code,
                   cr_patient_treatment_wrk.source_system_code AS source_system_code,
                   cr_patient_treatment_wrk.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_treatment_wrk) AS ms ON mt.treatment_id = ms.treatment_id
AND (coalesce(mt.tumor_id, 0) = coalesce(ms.tumor_id, 0)
     AND coalesce(mt.tumor_id, 1) = coalesce(ms.tumor_id, 1))
AND (coalesce(mt.treatment_hospital_id, 0) = coalesce(ms.treatment_hospital_id, 0)
     AND coalesce(mt.treatment_hospital_id, 1) = coalesce(ms.treatment_hospital_id, 1))
AND (coalesce(mt.treatment_type_id, 0) = coalesce(ms.treatment_type_id, 0)
     AND coalesce(mt.treatment_type_id, 1) = coalesce(ms.treatment_type_id, 1))
AND (coalesce(mt.surgical_site_id, 0) = coalesce(ms.surgical_site_id, 0)
     AND coalesce(mt.surgical_site_id, 1) = coalesce(ms.surgical_site_id, 1))
AND (coalesce(mt.surgical_margin_result_id, 0) = coalesce(ms.surgical_margin_result_id, 0)
     AND coalesce(mt.surgical_margin_result_id, 1) = coalesce(ms.surgical_margin_result_id, 1))
AND (coalesce(mt.treatment_type_group_id, 0) = coalesce(ms.treatment_type_group_id, 0)
     AND coalesce(mt.treatment_type_group_id, 1) = coalesce(ms.treatment_type_group_id, 1))
AND (coalesce(mt.clinical_trial_start_date, DATE '1970-01-01') = coalesce(ms.clinical_trial_start_date, DATE '1970-01-01')
     AND coalesce(mt.clinical_trial_start_date, DATE '1970-01-02') = coalesce(ms.clinical_trial_start_date, DATE '1970-01-02'))
AND (coalesce(mt.treatment_start_date, DATE '1970-01-01') = coalesce(ms.treatment_start_date, DATE '1970-01-01')
     AND coalesce(mt.treatment_start_date, DATE '1970-01-02') = coalesce(ms.treatment_start_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.clinical_trial_text, '0')) = upper(coalesce(ms.clinical_trial_text, '0'))
     AND upper(coalesce(mt.clinical_trial_text, '1')) = upper(coalesce(ms.clinical_trial_text, '1')))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (upper(coalesce(mt.treatment_performing_physician_code, '0')) = upper(coalesce(ms.treatment_performing_physician_code, '0'))
     AND upper(coalesce(mt.treatment_performing_physician_code, '1')) = upper(coalesce(ms.treatment_performing_physician_code, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (treatment_id,
        tumor_id,
        treatment_hospital_id,
        treatment_type_id,
        surgical_site_id,
        surgical_margin_result_id,
        treatment_type_group_id,
        clinical_trial_start_date,
        treatment_start_date,
        clinical_trial_text,
        comment_text,
        treatment_performing_physician_code,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.treatment_id, ms.tumor_id, ms.treatment_hospital_id, ms.treatment_type_id, ms.surgical_site_id, ms.surgical_margin_result_id, ms.treatment_type_group_id, ms.clinical_trial_start_date, ms.treatment_start_date, ms.clinical_trial_text, ms.comment_text, ms.treatment_performing_physician_code, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT treatment_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_treatment
      GROUP BY treatment_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_treatment');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_PATIENT_TREATMENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;