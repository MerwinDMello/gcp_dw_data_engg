-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_clinical_trial.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_clinical_trial AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    coalesce(cptt.clinical_trial_start_date, cpct.clinical_trial_enrolled_date) AS clinical_trial_enrolled_date,
    CASE
      WHEN cptt.clinical_trial_start_date IS NOT NULL THEN 'Y'
      WHEN cpct.clinical_trial_enrolled_ind IS NOT NULL THEN cpct.clinical_trial_enrolled_ind
      ELSE 'N'
    END AS clinical_trial_enrolled_ind,
    coalesce(cptt.clinical_trial_text, cpct.clinical_trial_name) AS clinical_trial_name,
    cpct.clinical_trial_offered_ind,
    cpct.clinical_trial_offered_date,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_clinical_trial AS cpct ON cptd.cn_patient_id = cpct.nav_patient_id
     AND cptd.cn_tumor_type_id = cpct.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_treatment AS cptt ON cpt.tumor_id = cptt.tumor_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cptt.tumor_id IS NOT NULL
   OR cpct.nav_patient_id IS NOT NULL
;
