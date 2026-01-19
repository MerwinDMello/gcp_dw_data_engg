-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_brca_screening_assessment.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_brca_screening_assessment AS SELECT
    a.brca_screening_assessment_sid,
    a.nav_patient_id,
    a.tumor_type_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.early_onset_breast_cancer_ind,
    a.ovarian_cancer_history_ind,
    a.two_primary_breast_cancer_ind,
    a.male_breast_cancer_ind,
    a.triple_negative_ind,
    a.ashkenazi_jewish_ind,
    a.two_plus_relative_cancer_ind,
    a.meeting_assessment_critieria_ind,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_brca_screening_assessment AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
