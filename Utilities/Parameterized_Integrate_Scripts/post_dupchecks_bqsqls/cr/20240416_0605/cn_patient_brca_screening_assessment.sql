DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_brca_screening_assessment.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_BRCA_SCREENING_ASSESSMENT	              	#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_PATIENT_BRCA_SCREENING_ASSESSMENT_STG#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_BRCA_SCREENING_ASSESSMENT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_patient_BRCA_Screening_Assessment_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_brca_screening_assessment
WHERE upper(trim(cn_patient_brca_screening_assessment.hashbite_ssk)) NOT IN
    (SELECT upper(trim(cn_patient_brca_screening_assessment_stg.hashbite_ssk))
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_brca_screening_assessment_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_brca_screening_assessment AS mt USING
  (SELECT DISTINCT cn_patient_brca_screening_assessment_stg.brca_screening_assessment_sid,
                   cn_patient_brca_screening_assessment_stg.nav_patient_id,
                   cn_patient_brca_screening_assessment_stg.tumor_type_id,
                   cn_patient_brca_screening_assessment_stg.navigator_id,
                   cn_patient_brca_screening_assessment_stg.coid AS coid,
                   cn_patient_brca_screening_assessment_stg.company_code AS company_code,
                   cn_patient_brca_screening_assessment_stg.early_onset_breast_cancer_ind AS early_onset_breast_cancer_ind,
                   cn_patient_brca_screening_assessment_stg.ovarian_cancer_history_ind AS ovarian_cancer_history_ind,
                   cn_patient_brca_screening_assessment_stg.two_primary_breast_cancer_ind AS two_primary_breast_cancer_ind,
                   cn_patient_brca_screening_assessment_stg.male_breast_cancer_ind AS male_breast_cancer_ind,
                   cn_patient_brca_screening_assessment_stg.triple_negative_ind AS triple_negative_ind,
                   cn_patient_brca_screening_assessment_stg.ashkenazi_jewish_ind AS ashkenazi_jewish_ind,
                   cn_patient_brca_screening_assessment_stg.two_plus_relative_cancer_ind AS two_plus_relative_cancer_ind,
                   cn_patient_brca_screening_assessment_stg.meeting_assmnt_critieria_ind AS meeting_assessment_critieria_ind,
                   cn_patient_brca_screening_assessment_stg.hashbite_ssk,
                   cn_patient_brca_screening_assessment_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_brca_screening_assessment_stg
   WHERE upper(cn_patient_brca_screening_assessment_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_brca_screening_assessment.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_brca_screening_assessment) ) AS ms ON mt.brca_screening_assessment_sid = ms.brca_screening_assessment_sid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.early_onset_breast_cancer_ind, '0')) = upper(coalesce(ms.early_onset_breast_cancer_ind, '0'))
     AND upper(coalesce(mt.early_onset_breast_cancer_ind, '1')) = upper(coalesce(ms.early_onset_breast_cancer_ind, '1')))
AND (upper(coalesce(mt.ovarian_cancer_history_ind, '0')) = upper(coalesce(ms.ovarian_cancer_history_ind, '0'))
     AND upper(coalesce(mt.ovarian_cancer_history_ind, '1')) = upper(coalesce(ms.ovarian_cancer_history_ind, '1')))
AND (upper(coalesce(mt.two_primary_breast_cancer_ind, '0')) = upper(coalesce(ms.two_primary_breast_cancer_ind, '0'))
     AND upper(coalesce(mt.two_primary_breast_cancer_ind, '1')) = upper(coalesce(ms.two_primary_breast_cancer_ind, '1')))
AND (upper(coalesce(mt.male_breast_cancer_ind, '0')) = upper(coalesce(ms.male_breast_cancer_ind, '0'))
     AND upper(coalesce(mt.male_breast_cancer_ind, '1')) = upper(coalesce(ms.male_breast_cancer_ind, '1')))
AND (upper(coalesce(mt.triple_negative_ind, '0')) = upper(coalesce(ms.triple_negative_ind, '0'))
     AND upper(coalesce(mt.triple_negative_ind, '1')) = upper(coalesce(ms.triple_negative_ind, '1')))
AND (upper(coalesce(mt.ashkenazi_jewish_ind, '0')) = upper(coalesce(ms.ashkenazi_jewish_ind, '0'))
     AND upper(coalesce(mt.ashkenazi_jewish_ind, '1')) = upper(coalesce(ms.ashkenazi_jewish_ind, '1')))
AND (upper(coalesce(mt.two_plus_relative_cancer_ind, '0')) = upper(coalesce(ms.two_plus_relative_cancer_ind, '0'))
     AND upper(coalesce(mt.two_plus_relative_cancer_ind, '1')) = upper(coalesce(ms.two_plus_relative_cancer_ind, '1')))
AND (upper(coalesce(mt.meeting_assessment_critieria_ind, '0')) = upper(coalesce(ms.meeting_assessment_critieria_ind, '0'))
     AND upper(coalesce(mt.meeting_assessment_critieria_ind, '1')) = upper(coalesce(ms.meeting_assessment_critieria_ind, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (brca_screening_assessment_sid,
        nav_patient_id,
        tumor_type_id,
        navigator_id,
        coid,
        company_code,
        early_onset_breast_cancer_ind,
        ovarian_cancer_history_ind,
        two_primary_breast_cancer_ind,
        male_breast_cancer_ind,
        triple_negative_ind,
        ashkenazi_jewish_ind,
        two_plus_relative_cancer_ind,
        meeting_assessment_critieria_ind,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.brca_screening_assessment_sid, ms.nav_patient_id, ms.tumor_type_id, ms.navigator_id, ms.coid, ms.company_code, ms.early_onset_breast_cancer_ind, ms.ovarian_cancer_history_ind, ms.two_primary_breast_cancer_ind, ms.male_breast_cancer_ind, ms.triple_negative_ind, ms.ashkenazi_jewish_ind, ms.two_plus_relative_cancer_ind, ms.meeting_assessment_critieria_ind, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT brca_screening_assessment_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_brca_screening_assessment
      GROUP BY brca_screening_assessment_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_brca_screening_assessment');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_BRCA_SCREENING_ASSESSMENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;