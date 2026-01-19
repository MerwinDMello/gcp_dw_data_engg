-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_heme_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_heme_diagnosis AS SELECT
    a.cn_patient_heme_diagnosis_sid,
    a.nav_patient_id,
    a.disease_status_id,
    a.tumor_type_id,
    a.diagnosis_result_id,
    a.nav_diagnosis_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.speciman_date,
    a.disease_diagnosis_text,
    a.therapy_related_ind,
    a.transformed_from_mds_ind,
    a.mipi_text,
    a.ipi_text,
    a.flipi_text,
    a.aids_related_ind,
    a.comment_text,
    a.classification_text,
    a.sub_classification_text,
    a.nhl_type_text,
    a.other_nhl_type_text,
    a.transformed_disease_text,
    a.non_malignant_type_text,
    a.feature_text,
    a.risk_category_text,
    a.mds_disease_risk_text,
    a.staging_field_1_text,
    a.staging_field_2_text,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_diagnosis AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
