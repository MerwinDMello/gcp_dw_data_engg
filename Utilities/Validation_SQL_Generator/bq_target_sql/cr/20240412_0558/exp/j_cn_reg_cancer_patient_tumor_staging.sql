-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_reg_cancer_patient_tumor_staging.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY a.cancer_patient_tumor_driver_sk, a.cancer_patient_driver_sk, a.cancer_tumor_driver_sk, upper(a.cancer_stage_class_method_code), upper(cancer_stage_type_code)) AS cancer_patient_tumor_staging_sk,
          a.cancer_patient_tumor_driver_sk,
          a.cancer_patient_driver_sk,
          a.cancer_tumor_driver_sk,
          a.coid,
          a.company_code,
          a.best_cs_summary_desc,
          a.best_cs_tnm_desc,
          a.tumor_size_num_text,
          a.cancer_stage_code,
          a.cancer_stage_class_method_code,
          cancer_stage_type_code AS cancer_stage_type_code,
          a.cancer_stage_result_text,
          a.ajcc_stage_desc,
          a.tumor_size_summary_desc,
          a.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
                     AND upper(rtrim(cps.cancer_stage_type_code)) = 'T'
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
                 AND upper(rtrim(cnps.cancer_staging_type_code)) = 'T'
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
                     AND upper(rtrim(cps.cancer_stage_type_code)) = 'N'
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
                 AND upper(rtrim(cnps.cancer_staging_type_code)) = 'N'
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
                     AND upper(rtrim(cps.cancer_stage_type_code)) = 'M'
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
                 AND upper(rtrim(cnps.cancer_staging_type_code)) = 'M'
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'C'
                     AND cps.cancer_stage_type_code IS NULL
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'C'
                 AND cnps.cancer_staging_type_code IS NULL
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
                     AND upper(rtrim(cps.cancer_stage_type_code)) = 'T'
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
                 AND upper(rtrim(cnps.cancer_staging_type_code)) = 'T'
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
                     AND upper(rtrim(cps.cancer_stage_type_code)) = 'N'
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
                 AND upper(rtrim(cnps.cancer_staging_type_code)) = 'N'
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
                     AND upper(rtrim(cps.cancer_stage_type_code)) = 'M'
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
                 AND upper(rtrim(cnps.cancer_staging_type_code)) = 'M'
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                cptd.cancer_patient_tumor_driver_sk,
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
              FROM
                `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_tumor_driver AS cptd
                LEFT OUTER JOIN (
                  SELECT
                      cpt.cr_patient_id,
                      cpt.tumor_primary_site_id,
                      t26.lookup_desc AS best_cs_summary_desc,
                      t46.lookup_desc AS best_cs_tnm_desc,
                      cpt.tumor_size_num_text,
                      cps.cancer_stage_classification_method_code,
                      cps.cancer_stage_type_code,
                      cps.cancer_stage_result_text,
                      ras.ajcc_stage_desc,
                      t43.lookup_desc AS tumor_size_summary_desc
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_staging AS cps ON cpt.tumor_id = cps.tumor_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_ajcc_stage AS ras ON cps.ajcc_stage_id = ras.ajcc_stage_id
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t26 ON cpt.best_cs_summary_id = t26.master_lookup_sid
                       AND t26.lookup_id = 26
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t46 ON cpt.best_cs_tnm_id = t46.master_lookup_sid
                       AND t46.lookup_id = 46
                      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_lookup_code AS t43 ON cpt.tumor_size_summary_id = t43.master_lookup_sid
                       AND t43.lookup_id = 43
                    WHERE upper(rtrim(cps.cancer_stage_classification_method_code)) = 'P'
                     AND cps.cancer_stage_type_code IS NULL
                ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
                 AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
                LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_staging AS cnps ON cptd.cn_patient_id = cnps.nav_patient_id
                 AND cptd.cn_tumor_type_id = cnps.tumor_type_id
                 AND upper(rtrim(cnps.cancer_stage_class_method_code)) = 'P'
                 AND cnps.cancer_staging_type_code IS NULL
              WHERE cr.cr_patient_id IS NOT NULL
               OR cnps.nav_patient_id IS NOT NULL
          ) AS a
          CROSS JOIN UNNEST(ARRAY[
            a.cancer_staging_type_code
          ]) AS cancer_stage_type_code
    ) AS a
;
