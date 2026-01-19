-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_genetics_assessment.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_genetics_assessment AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cbsa.ashkenazi_jewish_ind,
    cbsa.early_onset_breast_cancer_ind,
    cbsa.male_breast_cancer_ind,
    cbsa.ovarian_cancer_history_ind,
    cbsa.meeting_assessment_critieria_ind,
    cbsa.triple_negative_ind,
    cbsa.two_plus_relative_cancer_ind,
    cbsa.two_primary_breast_cancer_ind,
    rbct.breast_cancer_type_desc,
    cpgt.comment_text AS genetics_comment_text,
    rcrt.core_record_type_desc AS genetics_core_record_type_desc,
    cpgt.testing_date AS genetics_testing_date,
    cpgt.genetics_specialist_name,
    cpgt.test_name AS genetics_test_name,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_brca_screening_assessment AS cbsa ON cptd.cn_patient_id = cbsa.nav_patient_id
     AND cptd.cn_tumor_type_id = cbsa.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_genetics_testing AS cpgt ON cbsa.nav_patient_id = cpgt.nav_patient_id
     AND cbsa.tumor_type_id = cpgt.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_core_record_type AS rcrt ON cpgt.core_record_type_id = rcrt.core_record_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_breast_cancer_type AS rbct ON cpgt.breast_cancer_type_id = rbct.breast_cancer_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cbsa.nav_patient_id IS NOT NULL
   OR cpgt.nav_patient_id IS NOT NULL
;
