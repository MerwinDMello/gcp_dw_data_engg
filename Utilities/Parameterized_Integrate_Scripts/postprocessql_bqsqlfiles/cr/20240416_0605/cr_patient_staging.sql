DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_staging.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_STAGING		             	    #
-- #  TARGET  DATABASE	   	: EDWCR	 				    #
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_STAGING_WRK		    #
-- #	                                                                           #
-- #  INITIAL RELEASE	   	: 						    #
-- #  PROJECT             	: 	 		    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                                  #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_STAGING;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_STAGING_SAMPLE_STAGE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','REF_TUMOR_STAGE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_AJCC_STAGE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_staging_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_staging_wrk (cr_patient_staging_sid, cr_patient_id, tumor_id, ajcc_stage_id, cancer_stage_classification_method_code, cancer_stage_type_code, cancer_stage_result_text, source_system_code, dw_last_update_date_time)
SELECT row_number() OVER (
                          ORDER BY stg.tumorid,
                                   stg.patientid DESC) AS cr_patient_staging_sid,
                         stg.patientid AS cr_patient_id,
                         stg.tumorid AS tumor_id,
                         ajcc.ajcc_stage_id AS ajcc_stage_id,
                         stg.cancer_stge_clsfctn_mthd_cde AS cancer_stage_classification_me,
                         stg.cancer_stage_type_code AS cancer_stage_type_code,
                         upper(stg.clin_t_tnm) AS cancer_stage_result_text,
                         stg.source_system_code,
                         stg.dw_last_update_date_time
FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_staging_sample_stage AS stg
LEFT OUTER JOIN --  LEFT JOIN EDWCR_STAGING.REF_TUMOR_STAGE TR
 -- 	ON STG.TUMORID=TR.TUMORID1
 -- LEFT JOIN EDWCR.REF_AJCC_STAGE AJCC
 `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ajcc ON upper(rtrim(stg.code)) = upper(rtrim(ajcc.ajcc_stage_code))
AND stg.sub = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(ajcc.ajcc_stage_sub_code) AS FLOAT64)
AND stg.group1 = ajcc.ajcc_stage_group_id;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_STAGING_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_staging;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_staging AS mt USING
  (SELECT DISTINCT cr_patient_staging_wrk.cr_patient_staging_sid,
                   cr_patient_staging_wrk.cr_patient_id,
                   cr_patient_staging_wrk.tumor_id,
                   cr_patient_staging_wrk.ajcc_stage_id,
                   max(cr_patient_staging_wrk.cancer_stage_classification_method_code) AS cancer_stage_classification_method_code,
                   max(cr_patient_staging_wrk.cancer_stage_type_code) AS cancer_stage_type_code,
                   max(cr_patient_staging_wrk.cancer_stage_result_text) AS cancer_stage_result_text,
                   max(cr_patient_staging_wrk.source_system_code) AS source_system_code,
                   cr_patient_staging_wrk.dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_staging_wrk
   GROUP BY 1,
            2,
            3,
            4,
            upper(cr_patient_staging_wrk.cancer_stage_classification_method_code),
            upper(cr_patient_staging_wrk.cancer_stage_type_code),
            upper(cr_patient_staging_wrk.cancer_stage_result_text),
            upper(cr_patient_staging_wrk.source_system_code),
            9) AS ms ON mt.cr_patient_staging_sid = ms.cr_patient_staging_sid
AND (coalesce(mt.cr_patient_id, 0) = coalesce(ms.cr_patient_id, 0)
     AND coalesce(mt.cr_patient_id, 1) = coalesce(ms.cr_patient_id, 1))
AND (coalesce(mt.tumor_id, 0) = coalesce(ms.tumor_id, 0)
     AND coalesce(mt.tumor_id, 1) = coalesce(ms.tumor_id, 1))
AND (coalesce(mt.ajcc_stage_id, 0) = coalesce(ms.ajcc_stage_id, 0)
     AND coalesce(mt.ajcc_stage_id, 1) = coalesce(ms.ajcc_stage_id, 1))
AND (upper(coalesce(mt.cancer_stage_classification_method_code, '0')) = upper(coalesce(ms.cancer_stage_classification_method_code, '0'))
     AND upper(coalesce(mt.cancer_stage_classification_method_code, '1')) = upper(coalesce(ms.cancer_stage_classification_method_code, '1')))
AND (upper(coalesce(mt.cancer_stage_type_code, '0')) = upper(coalesce(ms.cancer_stage_type_code, '0'))
     AND upper(coalesce(mt.cancer_stage_type_code, '1')) = upper(coalesce(ms.cancer_stage_type_code, '1')))
AND (upper(coalesce(mt.cancer_stage_result_text, '0')) = upper(coalesce(ms.cancer_stage_result_text, '0'))
     AND upper(coalesce(mt.cancer_stage_result_text, '1')) = upper(coalesce(ms.cancer_stage_result_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cr_patient_staging_sid,
        cr_patient_id,
        tumor_id,
        ajcc_stage_id,
        cancer_stage_classification_method_code,
        cancer_stage_type_code,
        cancer_stage_result_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cr_patient_staging_sid, ms.cr_patient_id, ms.tumor_id, ms.ajcc_stage_id, ms.cancer_stage_classification_method_code, ms.cancer_stage_type_code, ms.cancer_stage_result_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cr_patient_staging_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_staging
      GROUP BY cr_patient_staging_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_staging');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CR_PATIENT_STAGING');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF