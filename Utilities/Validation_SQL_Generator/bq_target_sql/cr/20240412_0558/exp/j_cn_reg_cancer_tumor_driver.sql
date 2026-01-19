-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_reg_cancer_tumor_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY a.cr_tumor_primary_site_id, a.cn_tumor_type_id, a.cn_general_tumor_type_id, a.cn_navque_tumor_type_id, upper(a.cp_icd_oncology_code)) AS cancer_tumor_driver_sk,
          a.*
        FROM
          (
            SELECT DISTINCT
                coalesce(t3.icd_oncology_code, '-99') AS cp_icd_oncology_code,
                coalesce(t3.icd_oncology_site_desc, 'Unknown Description') AS cp_icd_oncology_site_desc,
                coalesce(coalesce(t7.tumor_group, t3.icd_oncology_site_desc), 'Unknown Description') AS cp_icd_oncology_group_name,
                t1.master_lookup_sid AS cr_tumor_primary_site_id,
                t4.tumor_type_id AS cn_tumor_type_id,
                t5.tumor_type_id AS cn_general_tumor_type_id,
                t6.tumor_type_id AS cn_navque_tumor_type_id,
                -- ,t1.Lookup_Id
                t1.lookup_code AS cr_icd_oncology_code,
                t1.lookup_desc AS cr_icd_oncology_site_desc,
                -- ,t3.ICD_Oncology_Code AS PTID_Primary_ICD_Oncology_Code_Lookup_ID
                -- ,t3.ICD_Oncology_Site_Desc AS PTID_Primary_ICD_Oncology_Code_Desc
                t4.tumor_type_group_name AS cn_tumor_group_name,
                t4.tumor_type_desc AS cn_tumor_type_desc,
                t5.tumor_type_group_name AS cn_general_tumor_group_name,
                t5.tumor_type_desc AS cn_general_tumor_type_desc,
                t6.tumor_type_desc AS cn_navque_tumor_type_desc,
                coalesce(t1.source_system_code, t3.source_system_code, t4.source_system_code, t5.source_system_code, t6.source_system_code, t7.source_system_code) AS source_system_code,
                datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
              FROM
                -- PT ID TumorTypes
                (
                  SELECT DISTINCT
                      t4_0.*
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_views.cancer_patient_id_output AS t1_0
                      INNER JOIN `hca-hin-dev-cur-ops`.edwcr_views.ref_icd_oncology AS t4_0 ON upper(rtrim(t4_0.icd_oncology_code)) = upper(rtrim(t1_0.submitted_primary_icd_oncology_code))
                       AND upper(rtrim(t4_0.icd_oncology_code)) NOT IN(
                        'C421'
                      )
                ) AS t3
                FULL OUTER JOIN -- CR TumorTypes
                `hca-hin-dev-cur-ops`.edwcr_views.ref_lookup_code AS t1 ON upper(rtrim(substr(t1.lookup_code, 1, 3))) = upper(rtrim(substr(t3.icd_oncology_code, 1, 3)))
                LEFT OUTER JOIN -- Navigation Tumor Module Lookup
                (
                  SELECT
                      CASE
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'BREAST'
                        ) THEN 'C509'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'LUNG'
                        ) THEN 'C349'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'HEMATOLOGY'
                        ) THEN 'C424'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI ANAL'
                        ) THEN 'C218'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI BILE DUCT'
                        ) THEN 'C249'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI COLON'
                        ) THEN 'C189'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI ESOPHAGEAL'
                        ) THEN 'C159'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI GASTRIC'
                        ) THEN 'C169'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI LIVER'
                        ) THEN 'C220'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI PANCREATIC'
                        ) THEN 'C259'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI RECTAL'
                        ) THEN 'C209'
                        WHEN upper(rtrim(t1_0.tumor_type_desc)) IN(
                          'GI MIXED TUMOR'
                        ) THEN 'C269'
                      END AS nav_icd_xwalk,
                      t1_0.*
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_views.ref_tumor_type AS t1_0
                    WHERE upper(rtrim(t1_0.tumor_type_group_name)) NOT IN(
                      'GENERAL', 'NAVQ', 'NULL', 'OTHER'
                    )
                     AND t1_0.tumor_type_group_name IS NOT NULL
                ) AS t4 ON upper(rtrim(substr(t1.lookup_code, 1, 3))) = upper(rtrim(substr(t4.nav_icd_xwalk, 1, 3)))
                LEFT OUTER JOIN -- Navigation General Tumor Look Up
                (
                  SELECT
                      CASE
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'ADRENAL'
                        ) THEN 'C749'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'BONE (OSTEOSARCOMA)'
                        ) THEN 'C419'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'CENTRAL NERVOUS SYSTEM (BRAIN, SPINAL CORD)'
                        ) THEN 'C710'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'CERVIX'
                        ) THEN 'C539'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'EWING SARCOMA'
                        ) THEN 'C499'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'EYE'
                        ) THEN 'C699'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'FALLOPIAN TUBE'
                        ) THEN 'C579'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'KAPOSI SARCOMA'
                        ) THEN 'C499'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'KIDNEY (RENAL)'
                        ) THEN 'C649'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'LACRIMAL GLAND'
                        ) THEN 'C695'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'LARYNX'
                        ) THEN 'C329'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'LUNG'
                        ) THEN 'C349'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'MELANOMA'
                        ) THEN 'C449'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'MERKEL CELL'
                        ) THEN 'C449'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'MESOTHELIOMA (NON-LUNG)'
                        ) THEN 'C809'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'MYELODYSPLASIA'
                        ) THEN 'C424'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'NASAL CAVITY'
                        ) THEN 'C300'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'NEURO ENDOCRINE'
                        ) THEN 'C809'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'ORAL'
                        ) THEN 'C069'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'OTHER'
                        ) THEN 'C809'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'OVARY'
                        ) THEN 'C569'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'PAROTID GLAND'
                        ) THEN 'C079'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'PENIS'
                        ) THEN 'C609'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'PERITONEUM'
                        ) THEN 'C482'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'PHARYNX'
                        ) THEN 'C148'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'PLACENTA'
                        ) THEN 'C589'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'PROSTATE'
                        ) THEN 'C619'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'SALIVARY GLAND'
                        ) THEN 'C089'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'SCROTUM'
                        ) THEN 'C639'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'SKIN (NON MELANOMA)'
                        ) THEN 'C449'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'SOFT TISSUE'
                        ) THEN 'C499'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'SUBMANDIBULAR GLAND'
                        ) THEN 'C779'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'THYMUS'
                        ) THEN 'C379'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'TESTIS'
                        ) THEN 'C629'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'THYROID'
                        ) THEN 'C739'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'TUMOR OF UNKNOWN PRIMARY'
                        ) THEN 'C809'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'URETER'
                        ) THEN 'C669'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'URETHRA'
                        ) THEN 'C689'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'UTERINE'
                        ) THEN 'C559'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'VAGINA'
                        ) THEN 'C529'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'VULVA'
                        ) THEN 'C519'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'BLADDER'
                        ) THEN 'C679'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'BREAST'
                        ) THEN 'C509'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI ANAL'
                        ) THEN 'C218'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI BILE DUCT'
                        ) THEN 'C249'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI COLON'
                        ) THEN 'C189'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI ESOPHAGEAL'
                        ) THEN 'C159'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI GASTRIC'
                        ) THEN 'C169'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI LIVER'
                        ) THEN 'C220'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI PANCREATIC'
                        ) THEN 'C259'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI RECTAL'
                        ) THEN 'C209'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GI MIXED TUMOR'
                        ) THEN 'C269'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'HEMATOLOGY'
                        ) THEN 'C424'
                        ELSE '-99'
                      END AS nav_general_icd_xwalk,
                      t1_0.*
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_views.ref_tumor_type AS t1_0
                    WHERE upper(rtrim(t1_0.tumor_type_group_name)) IN(
                      'GENERAL'
                    )
                ) AS t5 ON upper(rtrim(substr(t1.lookup_code, 1, 3))) = upper(rtrim(substr(t5.nav_general_icd_xwalk, 1, 3)))
                LEFT OUTER JOIN -- NavQue
                (
                  SELECT
                      CASE
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'GYN'
                        ) THEN 'C579'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'LUNG'
                        ) THEN 'C349'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'ACUTE LEUKEMIA'
                        ) THEN 'C424'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'BREAST'
                        ) THEN 'C509'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'COLON'
                        ) THEN 'C189'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'SARCOMA'
                        ) THEN 'C499'
                        WHEN upper(trim(t1_0.tumor_type_desc)) IN(
                          'COMPLEX GI'
                        ) THEN 'C269'
                      END AS navq_icd_xwalk,
                      t1_0.*
                    FROM
                      `hca-hin-dev-cur-ops`.edwcr_views.ref_tumor_type AS t1_0
                    WHERE upper(rtrim(t1_0.tumor_type_group_name)) IN(
                      'NAVQ'
                    )
                ) AS t6 ON upper(rtrim(substr(t1.lookup_code, 1, 3))) = upper(rtrim(substr(t6.navq_icd_xwalk, 1, 3)))
                LEFT OUTER JOIN -- Tumor Grouping
                (
                  SELECT
                      CASE
                        WHEN upper(rtrim(t2.icd_oncology_code)) IN(
                          'C23', 'C21', 'C24', 'C15', 'C22', 'C25', 'C20', 'C19', 'C16', 'C26', 'C17'
                        ) THEN 'Complex GI'
                        WHEN upper(rtrim(t2.icd_oncology_code)) IN(
                          'C51', 'C52', 'C53', 'C54', 'C55', 'C56', 'C57'
                        ) THEN 'Gynecological'
                      END AS tumor_group,
                      t2.icd_oncology_code,
                      t2.source_system_code
                    FROM
                      (
                        SELECT DISTINCT
                            substr(t1_0.icd_oncology_code, 1, 3) AS icd_oncology_code,
                            t1_0.icd_oncology_site_desc,
                            t1_0.source_system_code
                          FROM
                            `hca-hin-dev-cur-ops`.edwcr_views.ref_icd_oncology AS t1_0
                      ) AS t2
                ) AS t7 ON upper(rtrim(substr(t1.lookup_code, 1, 3))) = upper(rtrim(substr(t7.icd_oncology_code, 1, 3)))
              WHERE CAST(t1.lookup_id as FLOAT64) IN(
                18.0
              )
          ) AS a
    ) AS a
;
