DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_procedure_pathology_result.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT	              	#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result
WHERE (upper(cn_patient_procedure_pathology_result.hashbite_ssk),
       upper(cn_patient_procedure_pathology_result.navigation_procedure_type_code)) NOT IN
    (SELECT AS STRUCT upper(cn_patient_procedure_pathology_result_stg.hashbite_ssk) AS hashbite_ssk,
                      upper(cn_patient_procedure_pathology_result_stg.navigation_procedure_type_code) AS navigation_procedure_type_code
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_procedure_pathology_result_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY stg.cn_patient_procedure_sid,
                                               rre.nav_result_id,
                                               upper(stg.navigation_procedure_type_code),
                                               stg.pathology_result_date,
                                               upper(stg.pathology_result_name),
                                               upper(stg.pathology_grade_available_ind),
                                               stg.pathology_grade_num,
                                               upper(stg.pathology_tumor_size_av_ind),
                                               upper(stg.tumor_size_num_text),
                                               upper(stg.margin_result_detail_text),
                                               upper(stg.sentinel_node_result_code),
                                               stg.estrogen_receptor_sw,
                                               upper(stg.estrogen_receptor_st_cd),
                                               upper(stg.estrogen_receptor_pct_text),
                                               stg.progesterone_receptor_sw,
                                               upper(stg.progesterone_receptor_st_cd),
                                               upper(stg.progesterone_receptor_pct_text),
                                               upper(stg.oncotype_diagnosis_score_num),
                                               upper(stg.oncotype_diagnosis_risk_text),
                                               upper(stg.comment_text),
                                               upper(stg.hashbite_ssk)) +
     (SELECT coalesce(max(cn_patient_procedure_pathology_result.cn_patient_proc_pathology_result_sid), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result) AS cn_patient_proc_pathology_result_sid,
                                     stg.cn_patient_procedure_sid,
                                     rre1.nav_result_id,
                                     rre.nav_result_id AS nav_result_id_cw_1,
                                     rre2.nav_result_id AS nav_result_id_cw_2,
                                     stg.navigation_procedure_type_code AS navigation_procedure_type_code,
                                     stg.pathology_result_date,
                                     stg.pathology_result_name,
                                     stg.pathology_grade_available_ind AS pathology_grade_available_ind,
                                     stg.pathology_grade_num,
                                     stg.pathology_tumor_size_av_ind AS pathology_tumor_size_available_ind,
                                     stg.tumor_size_num_text,
                                     stg.margin_result_detail_text,
                                     stg.sentinel_node_result_code AS sentinel_node_result_code,
                                     stg.estrogen_receptor_sw,
                                     stg.estrogen_receptor_st_cd AS estrogen_receptor_strength_code,
                                     stg.estrogen_receptor_pct_text,
                                     stg.progesterone_receptor_sw,
                                     stg.progesterone_receptor_st_cd AS progesterone_receptor_strength_code,
                                     stg.progesterone_receptor_pct_text,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg.oncotype_diagnosis_score_num) AS INT64) AS oncotype_diagnosis_score_num,
                                     stg.oncotype_diagnosis_risk_text,
                                     substr(trim(stg.comment_text), 1, 2000) AS comment_text,
                                     stg.hashbite_ssk,
                                     stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_procedure_pathology_result_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_result AS rre ON upper(trim(stg.nav_result_id)) = upper(trim(rre.nav_result_desc))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_result AS rre1 ON upper(trim(stg.margin_result_id)) = upper(trim(rre1.nav_result_desc))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_result AS rre2 ON upper(trim(stg.oncotype_diagnosis_result_id)) = upper(trim(rre2.nav_result_desc))
   WHERE (upper(stg.hashbite_ssk),
          upper(stg.navigation_procedure_type_code)) NOT IN
       (SELECT AS STRUCT upper(cn_patient_procedure_pathology_result.hashbite_ssk) AS hashbite_ssk,
                         upper(cn_patient_procedure_pathology_result.navigation_procedure_type_code) AS navigation_procedure_type_code
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result) ) AS ms ON mt.cn_patient_proc_pathology_result_sid = ms.cn_patient_proc_pathology_result_sid
AND mt.cn_patient_procedure_sid = ms.cn_patient_procedure_sid
AND (coalesce(mt.margin_result_id, 0) = coalesce(ms.nav_result_id, 0)
     AND coalesce(mt.margin_result_id, 1) = coalesce(ms.nav_result_id, 1))
AND (coalesce(mt.nav_result_id, 0) = coalesce(ms.nav_result_id_cw_1, 0)
     AND coalesce(mt.nav_result_id, 1) = coalesce(ms.nav_result_id_cw_1, 1))
AND (coalesce(mt.oncotype_diagnosis_result_id, 0) = coalesce(ms.nav_result_id_cw_2, 0)
     AND coalesce(mt.oncotype_diagnosis_result_id, 1) = coalesce(ms.nav_result_id_cw_2, 1))
AND (upper(coalesce(mt.navigation_procedure_type_code, '0')) = upper(coalesce(ms.navigation_procedure_type_code, '0'))
     AND upper(coalesce(mt.navigation_procedure_type_code, '1')) = upper(coalesce(ms.navigation_procedure_type_code, '1')))
AND (coalesce(mt.pathology_result_date, DATE '1970-01-01') = coalesce(ms.pathology_result_date, DATE '1970-01-01')
     AND coalesce(mt.pathology_result_date, DATE '1970-01-02') = coalesce(ms.pathology_result_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.pathology_result_name, '0')) = upper(coalesce(ms.pathology_result_name, '0'))
     AND upper(coalesce(mt.pathology_result_name, '1')) = upper(coalesce(ms.pathology_result_name, '1')))
AND (upper(coalesce(mt.pathology_grade_available_ind, '0')) = upper(coalesce(ms.pathology_grade_available_ind, '0'))
     AND upper(coalesce(mt.pathology_grade_available_ind, '1')) = upper(coalesce(ms.pathology_grade_available_ind, '1')))
AND (coalesce(mt.pathology_grade_num, 0) = coalesce(ms.pathology_grade_num, 0)
     AND coalesce(mt.pathology_grade_num, 1) = coalesce(ms.pathology_grade_num, 1))
AND (upper(coalesce(mt.pathology_tumor_size_available_ind, '0')) = upper(coalesce(ms.pathology_tumor_size_available_ind, '0'))
     AND upper(coalesce(mt.pathology_tumor_size_available_ind, '1')) = upper(coalesce(ms.pathology_tumor_size_available_ind, '1')))
AND (upper(coalesce(mt.tumor_size_num_text, '0')) = upper(coalesce(ms.tumor_size_num_text, '0'))
     AND upper(coalesce(mt.tumor_size_num_text, '1')) = upper(coalesce(ms.tumor_size_num_text, '1')))
AND (upper(coalesce(mt.margin_result_detail_text, '0')) = upper(coalesce(ms.margin_result_detail_text, '0'))
     AND upper(coalesce(mt.margin_result_detail_text, '1')) = upper(coalesce(ms.margin_result_detail_text, '1')))
AND (upper(coalesce(mt.sentinel_node_result_code, '0')) = upper(coalesce(ms.sentinel_node_result_code, '0'))
     AND upper(coalesce(mt.sentinel_node_result_code, '1')) = upper(coalesce(ms.sentinel_node_result_code, '1')))
AND (coalesce(mt.estrogen_receptor_sw, 0) = coalesce(ms.estrogen_receptor_sw, 0)
     AND coalesce(mt.estrogen_receptor_sw, 1) = coalesce(ms.estrogen_receptor_sw, 1))
AND (upper(coalesce(mt.estrogen_receptor_strength_code, '0')) = upper(coalesce(ms.estrogen_receptor_strength_code, '0'))
     AND upper(coalesce(mt.estrogen_receptor_strength_code, '1')) = upper(coalesce(ms.estrogen_receptor_strength_code, '1')))
AND (upper(coalesce(mt.estrogen_receptor_pct_text, '0')) = upper(coalesce(ms.estrogen_receptor_pct_text, '0'))
     AND upper(coalesce(mt.estrogen_receptor_pct_text, '1')) = upper(coalesce(ms.estrogen_receptor_pct_text, '1')))
AND (coalesce(mt.progesterone_receptor_sw, 0) = coalesce(ms.progesterone_receptor_sw, 0)
     AND coalesce(mt.progesterone_receptor_sw, 1) = coalesce(ms.progesterone_receptor_sw, 1))
AND (upper(coalesce(mt.progesterone_receptor_strength_code, '0')) = upper(coalesce(ms.progesterone_receptor_strength_code, '0'))
     AND upper(coalesce(mt.progesterone_receptor_strength_code, '1')) = upper(coalesce(ms.progesterone_receptor_strength_code, '1')))
AND (upper(coalesce(mt.progesterone_receptor_pct_text, '0')) = upper(coalesce(ms.progesterone_receptor_pct_text, '0'))
     AND upper(coalesce(mt.progesterone_receptor_pct_text, '1')) = upper(coalesce(ms.progesterone_receptor_pct_text, '1')))
AND (coalesce(mt.oncotype_diagnosis_score_num, 0) = coalesce(ms.oncotype_diagnosis_score_num, 0)
     AND coalesce(mt.oncotype_diagnosis_score_num, 1) = coalesce(ms.oncotype_diagnosis_score_num, 1))
AND (upper(coalesce(mt.oncotype_diagnosis_risk_text, '0')) = upper(coalesce(ms.oncotype_diagnosis_risk_text, '0'))
     AND upper(coalesce(mt.oncotype_diagnosis_risk_text, '1')) = upper(coalesce(ms.oncotype_diagnosis_risk_text, '1')))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_proc_pathology_result_sid,
        cn_patient_procedure_sid,
        margin_result_id,
        nav_result_id,
        oncotype_diagnosis_result_id,
        navigation_procedure_type_code,
        pathology_result_date,
        pathology_result_name,
        pathology_grade_available_ind,
        pathology_grade_num,
        pathology_tumor_size_available_ind,
        tumor_size_num_text,
        margin_result_detail_text,
        sentinel_node_result_code,
        estrogen_receptor_sw,
        estrogen_receptor_strength_code,
        estrogen_receptor_pct_text,
        progesterone_receptor_sw,
        progesterone_receptor_strength_code,
        progesterone_receptor_pct_text,
        oncotype_diagnosis_score_num,
        oncotype_diagnosis_risk_text,
        comment_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_proc_pathology_result_sid, ms.cn_patient_procedure_sid, ms.nav_result_id, ms.nav_result_id_cw_1, ms.nav_result_id_cw_2, ms.navigation_procedure_type_code, ms.pathology_result_date, ms.pathology_result_name, ms.pathology_grade_available_ind, ms.pathology_grade_num, ms.pathology_tumor_size_available_ind, ms.tumor_size_num_text, ms.margin_result_detail_text, ms.sentinel_node_result_code, ms.estrogen_receptor_sw, ms.estrogen_receptor_strength_code, ms.estrogen_receptor_pct_text, ms.progesterone_receptor_sw, ms.progesterone_receptor_strength_code, ms.progesterone_receptor_pct_text, ms.oncotype_diagnosis_score_num, ms.oncotype_diagnosis_risk_text, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_proc_pathology_result_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result
      GROUP BY cn_patient_proc_pathology_result_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF