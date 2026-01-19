DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_reg_cancer_patient_tumor_staging.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.Cancer_Patient_Tumor_Staging           		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #
-- #	                                                                        	#
-- #
-- #
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_REG_CANCER_PATIENT_TUMOR_DRIVER;;
 --' FOR SESSION;;
 /* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cancer_patient_tumor_staging;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cancer_patient_tumor_staging AS mt USING
  (SELECT DISTINCT CAST(row_number() OVER (
                                           ORDER BY a.cancer_patient_tumor_driver_sk, a.cancer_patient_driver_sk, a.cancer_tumor_driver_sk, upper(a.cancer_stage_class_method_code), upper(cancer_stage_type_code)) AS NUMERIC) AS cancer_patient_tumor_staging_sk,
                   a.cancer_patient_tumor_driver_sk,
                   a.cancer_patient_driver_sk,
                   a.cancer_tumor_driver_sk,
                   a.coid AS coid,
                   a.company_code AS company_code,
                   a.best_cs_summary_desc,
                   a.best_cs_tnm_desc,
                   a.tumor_size_num_text AS tumor_size_num_text,
                   a.cancer_stage_code,
                   a.cancer_stage_class_method_code AS cancer_stage_class_method_code,
                   cancer_stage_type_code AS cancer_stage_type_code,
                   a.cancer_stage_result_text,
                   a.ajcc_stage_desc AS ajcc_stage_desc,
                   a.tumor_size_summary_desc,
                   a.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                      cptd.cancer_patient_driver_sk,
                      cptd.cancer_tumor_driver_sk,
                      cptd.coid,
                      cptd.company_code,
                      cr.best_cs_summary_desc,
                      cr.best_cs_tnm_desc,
                      cr.tumor_size_num_text,
                      cnps.cancer_stage_code,
                      coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                      coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                      coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                      cr.ajcc_stage_desc,
                      cr.tumor_size_summary_desc,
                      cptd.source_system_code,
                      cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'T' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'T'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'N' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'N'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'M' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'M'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
           AND cps.cancer_stage_type_code IS NULL ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
      AND cnps.cancer_staging_type_code IS NULL
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'T' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'T'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'N' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'N'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND upper(rtrim(cps.cancer_stage_type_code)) = 'M' ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND upper(rtrim(cnps.cancer_staging_type_code)) = 'M'
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL
      UNION ALL SELECT DISTINCT cptd.cancer_patient_tumor_driver_sk,
                                cptd.cancer_patient_driver_sk,
                                cptd.cancer_tumor_driver_sk,
                                cptd.coid,
                                cptd.company_code,
                                cr.best_cs_summary_desc,
                                cr.best_cs_tnm_desc,
                                cr.tumor_size_num_text,
                                cnps.cancer_stage_code,
                                coalesce(cr.cancer_stage_classification_method_code, cnps.cancer_stage_class_method_code) AS cancer_stage_class_method_code,
                                coalesce(cr.cancer_stage_type_code, cnps.cancer_staging_type_code) AS cancer_staging_type_code,
                                coalesce(cr.cancer_stage_result_text, cnps.cancer_staging_result_code) AS cancer_stage_result_text,
                                cr.ajcc_stage_desc,
                                cr.tumor_size_summary_desc,
                                cptd.source_system_code,
                                cptd.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN
        (SELECT cpt.cr_patient_id,
                cpt.tumor_primary_site_id,
                t26.lookup_desc AS best_cs_summary_desc,
                t46.lookup_desc AS best_cs_tnm_desc,
                cpt.tumor_size_num_text,
                cps.cancer_stage_classification_method_code,
                cps.cancer_stage_type_code,
                cps.cancer_stage_result_text,
                ras.ajcc_stage_desc,
                t43.lookup_desc AS tumor_size_summary_desc
         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
         AND t26.lookup_id = 26
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
         AND t46.lookup_id = 46
         LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
         AND t43.lookup_id = 43
         WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
           AND cps.cancer_stage_type_code IS NULL ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
      AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
      AND cptd.cn_tumor_type_id = cnps.tumor_type_id
      AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
      AND cnps.cancer_staging_type_code IS NULL
      WHERE cr.cr_patient_id IS NOT NULL
        OR cnps.nav_patient_id IS NOT NULL ) AS a
   CROSS JOIN UNNEST(ARRAY[ a.cancer_staging_type_code ]) AS cancer_stage_type_code) AS ms ON mt.cancer_patient_tumor_staging_sk = ms.cancer_patient_tumor_staging_sk
AND mt.cancer_patient_tumor_driver_sk = ms.cancer_patient_tumor_driver_sk
AND mt.cancer_patient_driver_sk = ms.cancer_patient_driver_sk
AND mt.cancer_tumor_driver_sk = ms.cancer_tumor_driver_sk
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.best_cs_summary_desc, '0')) = upper(coalesce(ms.best_cs_summary_desc, '0'))
     AND upper(coalesce(mt.best_cs_summary_desc, '1')) = upper(coalesce(ms.best_cs_summary_desc, '1')))
AND (upper(coalesce(mt.best_cs_tnm_desc, '0')) = upper(coalesce(ms.best_cs_tnm_desc, '0'))
     AND upper(coalesce(mt.best_cs_tnm_desc, '1')) = upper(coalesce(ms.best_cs_tnm_desc, '1')))
AND (upper(coalesce(mt.tumor_size_num_text, '0')) = upper(coalesce(ms.tumor_size_num_text, '0'))
     AND upper(coalesce(mt.tumor_size_num_text, '1')) = upper(coalesce(ms.tumor_size_num_text, '1')))
AND (upper(coalesce(mt.cancer_stage_code, '0')) = upper(coalesce(ms.cancer_stage_code, '0'))
     AND upper(coalesce(mt.cancer_stage_code, '1')) = upper(coalesce(ms.cancer_stage_code, '1')))
AND (upper(coalesce(mt.cancer_stage_class_method_code, '0')) = upper(coalesce(ms.cancer_stage_class_method_code, '0'))
     AND upper(coalesce(mt.cancer_stage_class_method_code, '1')) = upper(coalesce(ms.cancer_stage_class_method_code, '1')))
AND (upper(coalesce(mt.cancer_stage_type_code, '0')) = upper(coalesce(ms.cancer_stage_type_code, '0'))
     AND upper(coalesce(mt.cancer_stage_type_code, '1')) = upper(coalesce(ms.cancer_stage_type_code, '1')))
AND (upper(coalesce(mt.cancer_stage_result_text, '0')) = upper(coalesce(ms.cancer_stage_result_text, '0'))
     AND upper(coalesce(mt.cancer_stage_result_text, '1')) = upper(coalesce(ms.cancer_stage_result_text, '1')))
AND (upper(coalesce(mt.ajcc_stage_desc, '0')) = upper(coalesce(ms.ajcc_stage_desc, '0'))
     AND upper(coalesce(mt.ajcc_stage_desc, '1')) = upper(coalesce(ms.ajcc_stage_desc, '1')))
AND (upper(coalesce(mt.tumor_size_summary_desc, '0')) = upper(coalesce(ms.tumor_size_summary_desc, '0'))
     AND upper(coalesce(mt.tumor_size_summary_desc, '1')) = upper(coalesce(ms.tumor_size_summary_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_tumor_staging_sk,
        cancer_patient_tumor_driver_sk,
        cancer_patient_driver_sk,
        cancer_tumor_driver_sk,
        coid,
        company_code,
        best_cs_summary_desc,
        best_cs_tnm_desc,
        tumor_size_num_text,
        cancer_stage_code,
        cancer_stage_class_method_code,
        cancer_stage_type_code,
        cancer_stage_result_text,
        ajcc_stage_desc,
        tumor_size_summary_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_tumor_staging_sk, ms.cancer_patient_tumor_driver_sk, ms.cancer_patient_driver_sk, ms.cancer_tumor_driver_sk, ms.coid, ms.company_code, ms.best_cs_summary_desc, ms.best_cs_tnm_desc, ms.tumor_size_num_text, ms.cancer_stage_code, ms.cancer_stage_class_method_code, ms.cancer_stage_type_code, ms.cancer_stage_result_text, ms.ajcc_stage_desc, ms.tumor_size_summary_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_tumor_staging_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.cancer_patient_tumor_staging
      GROUP BY cancer_patient_tumor_staging_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cancer_patient_tumor_staging');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Cancer_Patient_Tumor_Staging');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;