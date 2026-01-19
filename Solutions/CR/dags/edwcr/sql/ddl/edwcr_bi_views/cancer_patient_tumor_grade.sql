-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_grade.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_grade AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cn.core_record_type_desc,
    CASE
      WHEN cn.core_record_type_desc IS NOT NULL THEN 'Y'
      ELSE 'N'
    END AS core_record_type_ind,
    coalesce(crg.lookup_desc, CASt(cn.pathology_grade_num AS STRING)) AS pathology_grade_text,
    cn.pathology_grade_available_ind,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN (
      SELECT
          rcrt.core_record_type_desc,
          nav_patient_id,
          tumor_type_id,
          pathology_grade_available_ind,
          pathology_grade_num
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery AS s
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS cppr ON s.cn_patient_surgery_sid = cppr.cn_patient_procedure_sid
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON s.core_record_type_id = rcrt.core_record_type_id
        WHERE upper(cppr.navigation_procedure_type_code) = 'S'
      UNION ALL
      SELECT
          rcrt.core_record_type_desc,
          nav_patient_id,
          tumor_type_id,
          pathology_grade_available_ind,
          pathology_grade_num
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_biopsy AS s
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS cppr ON s.cn_patient_biopsy_sid = cppr.cn_patient_procedure_sid
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON s.core_record_type_id = rcrt.core_record_type_id
        WHERE upper(cppr.navigation_procedure_type_code) = 'B'
    ) AS cn ON cptd.cn_patient_id = cn.nav_patient_id
     AND cptd.cn_tumor_type_id = cn.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cr ON cptd.cr_patient_id = cr.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
    LEFT OUTER JOIN (
      SELECT
          *
        FROM
          {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code
        WHERE ref_lookup_code.lookup_id IN(
          SELECT
              lookup_sid
            FROM
              {{ params.param_cr_base_views_dataset_name }}.ref_lookup_name
            WHERE upper(lookup_name) = 'GRADE'
        )
    ) AS crg ON cr.tumor_grade_id = crg.master_lookup_sid
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cr.cr_patient_id IS NOT NULL
   OR cn.nav_patient_id IS NOT NULL
;
