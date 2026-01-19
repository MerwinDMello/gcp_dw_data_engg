-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_reg_cancer_patient_tumor_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY a.cancer_patient_driver_sk, a.t_sk, a.cr_patient_id, a.cn_patient_id, a.cp_patient_id, a.cr_tumor_primary_site_id, a.cn_tumor_type_id, upper(a.cp_icd_oncology_code)) AS cancer_patient_tumor_driver_sk,
          a.*,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT DISTINCT
                cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN (
                  SELECT
                      cancer_tumor_driver.*
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver
                    QUALIFY row_number() OVER (PARTITION BY cancer_tumor_driver.cr_tumor_primary_site_id ORDER BY cancer_tumor_driver.cancer_tumor_driver_sk) = 1
                ) AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
              WHERE cpd.cn_patient_id IS NULL
               AND cpd.cp_patient_id IS NULL
            UNION DISTINCT
            SELECT DISTINCT
                /* CP only records */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t2.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                cpnt.patient_dw_id AS cp_patient_id,
                cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(cpnt.submitted_primary_icd_oncology_code)) = upper(rtrim(t2.cp_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NULL
               AND cpd.cn_patient_id IS NULL
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, upper(cpnt.submitted_primary_icd_oncology_code) ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT
                /* CN only records */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
              WHERE cpd.cr_patient_id IS NULL
               AND cpd.cp_patient_id IS NULL
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* CR and CP only */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                cpnt.patient_dw_id AS cp_patient_id,
                cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NULL
               AND t.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /*and
                CANCER_PATIENT_DRIVER_SK=2246473*/
                /* Records for Extra CP when CR and CP not null and CN null */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t2.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                cpnt.patient_dw_id AS cp_patient_id,
                cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NULL
               AND (cpnt.patient_dw_id, upper(cpnt.submitted_primary_icd_oncology_code)) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /*AND
                    CANCER_PATIENT_DRIVER_SK=2246473*/
                    cpnt_0.patient_dw_id,
                    upper(cpnt_0.submitted_primary_icd_oncology_code) AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt_0 ON cpd_0.cp_patient_id = cpnt_0.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt_0.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NULL
                   AND t.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, upper(cpnt.submitted_primary_icd_oncology_code) ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /*and
                CANCER_PATIENT_DRIVER_SK=2246473*/
                /* Records for Extra CR when CR and CP not null and CN null */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NULL
               AND (cpt.cr_patient_id, cpt.tumor_primary_site_id) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /*AND
                    CANCER_PATIENT_DRIVER_SK=2246473*/
                    cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt ON cpd_0.cp_patient_id = cpnt.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NULL
                   AND t_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /*and
                CANCER_PATIENT_DRIVER_SK=2246473*/
                /* CR and CN only */ /* Records when CR and CN not null and CP null and tumor matches*/ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND t.cancer_tumor_driver_sk = t1.cancer_tumor_driver_sk
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT
                /*and cancer_patient_driver_sk=1872902*/
                /* Records for Extra CN when CR and CN not null and CP null */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND (cpnt.nav_patient_id, cpnt.tumor_type_id) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /*AND
                    CANCER_PATIENT_DRIVER_SK=294059*/
                    cpnt_0.nav_patient_id,
                    cpnt_0.tumor_type_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND t.cancer_tumor_driver_sk = t1_0.cancer_tumor_driver_sk
              )
              QUALIFY /*and cancer_patient_driver_sk=294059*/ row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* Records for Extra CR when CR and CN not null and CP null */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND (cpt.cr_patient_id, cpt.tumor_primary_site_id) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /*AND
                    CANCER_PATIENT_DRIVER_SK=294059*/
                    cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt.tumor_type_id
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND t_0.cancer_tumor_driver_sk = t1.cancer_tumor_driver_sk
              )
              QUALIFY /*and cancer_patient_driver_sk=294059*/ row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                --  CP and CN not null and CR Null ---
                /* Both CN and CP have data and no data in CR*/ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                cpnt2.patient_dw_id AS cp_patient_id,
                cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND t1.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id, upper(cpnt2.submitted_primary_icd_oncology_code) ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT
                /* Extra data in CN where CN and CP are not null and CR is null */
                cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
              WHERE cpd.cr_patient_id IS NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND (cpnt.nav_patient_id, cpnt.tumor_type_id) NOT IN(
                SELECT DISTINCT AS STRUCT
                    cpnt_0.nav_patient_id,
                    cpnt_0.tumor_type_id
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND t1_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* Extra data in CP where CN and CP are not null and CR is null */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t2.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                cpnt2.patient_dw_id AS cp_patient_id,
                cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND (cpnt2.patient_dw_id, upper(cpnt2.submitted_primary_icd_oncology_code)) NOT IN(
                SELECT DISTINCT AS STRUCT
                    cpnt2_0.patient_dw_id,
                    upper(cpnt2_0.submitted_primary_icd_oncology_code) AS submitted_primary_icd_oncology_code
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND t1.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, upper(cpnt2.submitted_primary_icd_oncology_code) ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT
                /* all 3 match tumor types*/ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                cpnt2.patient_dw_id AS cp_patient_id,
                cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND /*and cancer_patient_driver_sk=107567*/ t1.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
               AND t.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
              QUALIFY row_number() OVER (PARTITION BY upper(cpnt2.submitted_primary_icd_oncology_code), cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT
                /* Matching CR and CN without CP match */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND t1.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
               AND (t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk
               OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
               AND (cpt.cr_patient_id, cpt.tumor_primary_site_id) NOT IN(
                SELECT AS STRUCT
                    /*and
                    CANCER_PATIENT_DRIVER_SK=104575*/
                    cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1_0.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
                   AND t_0.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id, cr_tumor_primary_site_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* Matching CP and CN data with CR not macthing */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                cpnt2.patient_dw_id AS cp_patient_id,
                cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND /*and cancer_patient_driver_sk=107567*/ t1.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
               AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
               OR t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
               AND (cpnt2.patient_dw_id, upper(cpnt2.submitted_primary_icd_oncology_code)) NOT IN(
                SELECT AS STRUCT
                    cpnt2_0.patient_dw_id,
                    upper(cpnt2_0.submitted_primary_icd_oncology_code) AS submitted_primary_icd_oncology_code
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1_0.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
                   AND t_0.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* CR and CP data with not mataching CN data */ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                cpnt2.patient_dw_id AS cp_patient_id,
                cpnt2.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt2 ON cpd.cp_patient_id = cpnt2.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND /*and cancer_patient_driver_sk=107567*/ t2.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
               AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
               OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
               AND (cpt.cr_patient_id, cpt.tumor_primary_site_id) NOT IN(
                SELECT AS STRUCT
                    cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2_0 ON cpd_0.cp_patient_id = cpnt2_0.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2_0.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1_0.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
                   AND t_0.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* All remaining CR records*/
                cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t.cancer_tumor_driver_sk AS t_sk,
                cpt.cr_patient_id,
                cpt.tumor_primary_site_id AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd.cr_patient_id = cpt.cr_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND /*and cancer_patient_driver_sk=107567*/ (cpt.cr_patient_id, cpt.tumor_primary_site_id) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /* exclude CR data if CR and CN match */ cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
                   AND (t_0.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk
                   OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
                UNION ALL
                SELECT DISTINCT AS STRUCT
                    /* exclude CR data if all 3 match */ cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
                   AND t_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
                UNION ALL
                SELECT DISTINCT AS STRUCT
                    /* exclude CR data if CR and CP match*/ cpt_0.cr_patient_id,
                    cpt_0.tumor_primary_site_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt_0 ON cpd_0.cr_patient_id = cpt_0.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t_0 ON cpt_0.tumor_primary_site_id = t_0.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd_0.cn_patient_id = cpnt.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t2.cancer_tumor_driver_sk = t_0.cancer_tumor_driver_sk
                   AND (t_0.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
                   OR t1.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cr_tumor_primary_site_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* All remaining CN data */
                cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t1.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                cpnt.nav_patient_id AS cn_patient_id,
                cpnt.tumor_type_id AS cn_tumor_type_id,
                CAST(NULL as NUMERIC) AS cp_patient_id,
                CAST(NULL as STRING) AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt ON cpd.cn_patient_id = cpnt.nav_patient_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt.tumor_type_id = rtt.tumor_type_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                   upper(rtrim(rtt.tumor_type_group_name))
                  WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                  WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                  ELSE t1.cn_tumor_type_id
                END = cpnt.tumor_type_id
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND /*and cancer_patient_driver_sk=107567*/ (cpnt.nav_patient_id, cpnt.tumor_type_id) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /* exclude CN data if CR and CN match*/ cpnt_0.nav_patient_id,
                    cpnt_0.tumor_type_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1_0.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
                   AND (t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk
                   OR t1_0.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
                UNION ALL
                SELECT DISTINCT AS STRUCT
                    /* exclude CN data if all 3 match*/ cpnt_0.nav_patient_id,
                    cpnt_0.tumor_type_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1_0.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
                   AND t.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
                UNION ALL
                SELECT DISTINCT AS STRUCT
                    /* exclude CN data if CN and CP match*/ cpnt_0.nav_patient_id,
                    cpnt_0.tumor_type_id AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt_0 ON cpnt_0.tumor_type_id = rtt_0.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1_0 ON CASE
                       upper(rtrim(rtt_0.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1_0.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1_0.cn_navque_tumor_type_id
                      ELSE t1_0.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1_0.cancer_tumor_driver_sk = t2.cancer_tumor_driver_sk
                   AND (t.cancer_tumor_driver_sk <> t1_0.cancer_tumor_driver_sk
                   OR t.cancer_tumor_driver_sk <> t2.cancer_tumor_driver_sk)
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, cn_tumor_type_id ORDER BY t_sk) = 1
            UNION DISTINCT
            SELECT DISTINCT
                /* All remaining CP data*/ cpd.coid,
                cpd.company_code,
                cpd.cancer_patient_driver_sk,
                t2.cancer_tumor_driver_sk AS t_sk,
                CAST(NULL as INT64) AS cr_patient_id,
                CAST(NULL as INT64) AS cr_tumor_primary_site_id,
                CAST(NULL as NUMERIC) AS cn_patient_id,
                CAST(NULL as INT64) AS cn_tumor_type_id,
                cpnt.patient_dw_id AS cp_patient_id,
                cpnt.submitted_primary_icd_oncology_code AS cp_icd_oncology_code,
                cpd.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd
                INNER JOIN (
                  SELECT DISTINCT
                      cancer_patient_id_output.patient_dw_id,
                      cancer_patient_id_output.submitted_primary_icd_oncology_code
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                ) AS cpnt ON cpd.cp_patient_id = cpnt.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2 ON upper(rtrim(t2.cp_icd_oncology_code)) = upper(rtrim(cpnt.submitted_primary_icd_oncology_code))
              WHERE cpd.cr_patient_id IS NOT NULL
               AND cpd.cp_patient_id IS NOT NULL
               AND cpd.cn_patient_id IS NOT NULL
               AND /*and cancer_patient_driver_sk=107567*/ (cpnt.patient_dw_id, upper(cpnt.submitted_primary_icd_oncology_code)) NOT IN(
                SELECT DISTINCT AS STRUCT
                    /* exclude CP data if CR and CP match*/ cpnt2.patient_dw_id,
                    upper(cpnt2.submitted_primary_icd_oncology_code) AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt_0.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t2_0.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
                   AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
                   OR t1.cancer_tumor_driver_sk <> t2_0.cancer_tumor_driver_sk)
                UNION ALL
                SELECT DISTINCT AS STRUCT
                    /* exclude CPdata if all 3 match*/ cpnt2.patient_dw_id,
                    upper(cpnt2.submitted_primary_icd_oncology_code) AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt_0.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1.cancer_tumor_driver_sk = t.cancer_tumor_driver_sk
                   AND t.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
                UNION ALL
                SELECT DISTINCT AS STRUCT
                    /* exclude CP data if CN and CP match*/ cpnt2.patient_dw_id,
                    upper(cpnt2.submitted_primary_icd_oncology_code) AS t1_sk
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.cancer_patient_driver AS cpd_0
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cr_patient_tumor AS cpt ON cpd_0.cr_patient_id = cpt.cr_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t ON cpt.tumor_primary_site_id = t.cr_tumor_primary_site_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_tumor AS cpnt_0 ON cpd_0.cn_patient_id = cpnt_0.nav_patient_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_tumor_type AS rtt ON cpnt_0.tumor_type_id = rtt.tumor_type_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t1 ON CASE
                       upper(rtrim(rtt.tumor_type_group_name))
                      WHEN 'GENERAL' THEN t1.cn_general_tumor_type_id
                      WHEN 'NAVQ' THEN t1.cn_navque_tumor_type_id
                      ELSE t1.cn_tumor_type_id
                    END = cpnt_0.tumor_type_id
                    INNER JOIN (
                      SELECT DISTINCT
                          cancer_patient_id_output.patient_dw_id,
                          cancer_patient_id_output.submitted_primary_icd_oncology_code
                        FROM
                          `hca-hin-dev-cur-ops`.edwcr_base_views.cancer_patient_id_output
                    ) AS cpnt2 ON cpd_0.cp_patient_id = cpnt2.patient_dw_id
                    INNER JOIN `hca-hin-dev-cur-ops`.edwcr.cancer_tumor_driver AS t2_0 ON upper(rtrim(t2_0.cp_icd_oncology_code)) = upper(rtrim(cpnt2.submitted_primary_icd_oncology_code))
                  WHERE cpd_0.cr_patient_id IS NOT NULL
                   AND cpd_0.cp_patient_id IS NOT NULL
                   AND cpd_0.cn_patient_id IS NOT NULL
                   AND /*and cancer_patient_driver_sk=107567*/ t1.cancer_tumor_driver_sk = t2_0.cancer_tumor_driver_sk
                   AND (t.cancer_tumor_driver_sk <> t1.cancer_tumor_driver_sk
                   OR t.cancer_tumor_driver_sk <> t2_0.cancer_tumor_driver_sk)
              )
              QUALIFY row_number() OVER (PARTITION BY cpd.cancer_patient_driver_sk, upper(cpnt.submitted_primary_icd_oncology_code) ORDER BY t_sk) = 1
          ) AS a
    ) AS a
;
