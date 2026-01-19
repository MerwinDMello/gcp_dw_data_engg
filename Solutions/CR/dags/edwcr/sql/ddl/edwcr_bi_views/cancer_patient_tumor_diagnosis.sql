-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_diagnosis AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cn.diagnosis_side_desc,
    coalesce(cpdd.diagnosis_date, cn.diagnosis_date) AS diagnosis_date,
    cn.general_diagnosis_name AS diagnosis_name,
    coalesce(t11.lookup_desc, cn.diagnosis_result_desc) AS diagnosis_result_desc,
    coalesce(cpdd.diagnose_age_num, cn.diagnose_age_num) AS diagnose_age_num,
    coalesce(t11.lookup_desc, cn.general_diagnosis_name) AS general_diagnosis_name,
    cn.diagnosis_indicator_text,
    cn.diagnosis_detail_desc,
    coalesce(t12.lookup_desc, cn.tumor_type_desc) AS tumor_site_desc,
    cpdd.first_diagnose_year_num,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_diagnosis_detail AS cpdd ON cpt.cr_patient_id = cpdd.cr_patient_id
     AND cpt.tumor_id = cpdd.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS t12 ON cpt.tumor_primary_site_id = t12.master_lookup_sid
     AND t12.lookup_id = 12
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_lookup_code AS t11 ON cpdd.diagnosis_name_id = t11.master_lookup_sid
     AND t11.lookup_id = 11
    LEFT OUTER JOIN (
      SELECT
          cpt_0.nav_patient_id,
          cpt_0.tumor_type_id,
          rs.side_desc AS diagnosis_side_desc,
          cpd.diagnosis_date,
          cpd.general_diagnosis_name,
          rdr.diagnosis_result_desc,
          rdd.diagnosis_indicator_text,
          rdd.diagnosis_detail_desc,
          rtt.tumor_type_desc,
          CASE
            WHEN DATE(cpd.diagnosis_date) = '1900-01-01' THEN CAST(0 as BigNumeric)
            ELSE DATE_DIFF(cpd.diagnosis_date, cp.birth_date,DAY) / 365
          END AS diagnose_age_num
        FROM
          {{ params.param_cr_core_dataset_name }}.cn_person AS cp
          LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_tumor AS cpt_0 ON cp.nav_patient_id = cpt_0.nav_patient_id
          LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_diagnosis AS cpd ON cpt_0.nav_patient_id = cpd.nav_patient_id
           AND cpt_0.tumor_type_id = cpd.tumor_type_id
          LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail AS rdd ON cpd.diagnosis_detail_id = rdd.diagnosis_detail_id
          LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_diagnosis_result AS rdr ON cpd.diagnosis_result_id = rdr.diagnosis_result_id
          LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_side AS rs ON cpd.diagnosis_side_id = rs.side_id
          LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_tumor_type AS rtt ON cpd.tumor_type_id = rtt.tumor_type_id
    ) AS cn ON cptd.cn_patient_id = cn.nav_patient_id
     AND cptd.cn_tumor_type_id = cn.tumor_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cpdd.cr_patient_id IS NOT NULL
   OR cn.nav_patient_id IS NOT NULL
;