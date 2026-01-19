DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_rad_onc_course_diagnosis.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_Rad_Onc_Course_Diagnosis                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_FACTCOURSEDIAGNOSIS 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Fact_Rad_Onc_Course_Diagnosis;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_FACTCOURSEDIAGNOSIS');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_course_diagnosis;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_course_diagnosis AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dp.dimsiteid,
                                               dp.factcoursediagnosisid DESC) AS fact_course_diagnosis_sk,
                                     ra.fact_patient_diagnosis_sk AS fact_patient_diagnosis_sk,
                                     ra1.patient_course_sk AS patient_course_sk,
                                     dc.diagnosis_code_sk AS diagnosis_code_sk,
                                     rr.site_sk AS site_sk,
                                     dp.factcoursediagnosisid AS source_fact_course_diagnosis_id,
                                     CASE dp.isprimary
                                         WHEN 1 THEN 'Y'
                                         WHEN 0 THEN 'N'
                                     END AS primary_course_ind,
                                     dp.logid AS log_id,
                                     dp.runid AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT stg_factcoursediagnosis.factcoursediagnosisid,
             stg_factcoursediagnosis.factpatientdiagnosisid,
             stg_factcoursediagnosis.dimcourseid,
             stg_factcoursediagnosis.dimdiagnosiscodeid,
             stg_factcoursediagnosis.dimsiteid,
             stg_factcoursediagnosis.isprimary,
             stg_factcoursediagnosis.logid,
             stg_factcoursediagnosis.runid
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factcoursediagnosis) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site) AS rr ON rr.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT fact_rad_onc_patient_diagnosis.source_fact_patient_diagnosis_id,
             fact_rad_onc_patient_diagnosis.fact_patient_diagnosis_sk,
             fact_rad_onc_patient_diagnosis.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.fact_rad_onc_patient_diagnosis) AS ra ON dp.factpatientdiagnosisid = ra.source_fact_patient_diagnosis_id
   AND rr.site_sk = ra.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient_course.source_patient_course_id,
             rad_onc_patient_course.patient_course_sk,
             rad_onc_patient_course.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient_course) AS ra1 ON dp.dimcourseid = ra1.source_patient_course_id
   AND rr.site_sk = ra1.site_sk
   LEFT OUTER JOIN
     (SELECT ref_rad_onc_diagnosis_code.source_diagnosis_code_id,
             ref_rad_onc_diagnosis_code.diagnosis_code_sk,
             ref_rad_onc_diagnosis_code.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_diagnosis_code) AS dc ON dp.dimdiagnosiscodeid = dc.source_diagnosis_code_id
   AND rr.site_sk = dc.site_sk) AS ms ON mt.fact_course_diagnosis_sk = ms.fact_course_diagnosis_sk
AND (coalesce(mt.fact_patient_diagnosis_sk, 0) = coalesce(ms.fact_patient_diagnosis_sk, 0)
     AND coalesce(mt.fact_patient_diagnosis_sk, 1) = coalesce(ms.fact_patient_diagnosis_sk, 1))
AND (coalesce(mt.patient_course_sk, 0) = coalesce(ms.patient_course_sk, 0)
     AND coalesce(mt.patient_course_sk, 1) = coalesce(ms.patient_course_sk, 1))
AND (coalesce(mt.diagnosis_code_sk, 0) = coalesce(ms.diagnosis_code_sk, 0)
     AND coalesce(mt.diagnosis_code_sk, 1) = coalesce(ms.diagnosis_code_sk, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_fact_course_diagnosis_id = ms.source_fact_course_diagnosis_id
AND (upper(coalesce(mt.primary_course_ind, '0')) = upper(coalesce(ms.primary_course_ind, '0'))
     AND upper(coalesce(mt.primary_course_ind, '1')) = upper(coalesce(ms.primary_course_ind, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (fact_course_diagnosis_sk,
        fact_patient_diagnosis_sk,
        patient_course_sk,
        diagnosis_code_sk,
        site_sk,
        source_fact_course_diagnosis_id,
        primary_course_ind,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.fact_course_diagnosis_sk, ms.fact_patient_diagnosis_sk, ms.patient_course_sk, ms.diagnosis_code_sk, ms.site_sk, ms.source_fact_course_diagnosis_id, ms.primary_course_ind, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT fact_course_diagnosis_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_course_diagnosis
      GROUP BY fact_course_diagnosis_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_course_diagnosis');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_Rad_Onc_Course_Diagnosis');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF