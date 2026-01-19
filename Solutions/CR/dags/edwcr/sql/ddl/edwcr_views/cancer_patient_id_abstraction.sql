-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cancer_patient_id_abstraction.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cancer_patient_id_abstraction AS SELECT
    a.cancer_abstraction_sk,
    a.cancer_patient_id_output_sk,
    a.message_control_id_text,
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.pat_acct_num,
    a.abstraction_report_assigned_date_time,
    a.abstraction_date_time,
    a.abstraction_action_user_3_4_id,
    a.abstraction_action_desc,
    a.abstraction_action_date_time,
    a.primary_icd_oncology_code,
    a.primary_icd_site_desc,
    a.primary_icd_site_and_model_score_desc,
    a.changed_primary_icd_oncology_code,
    a.changed_primary_icd_site_desc,
    a.topography_icd_oncology_code,
    a.topography_icd_site_desc,
    a.laterality_icd_oncology_code,
    a.laterality_icd_site_desc,
    a.secondary_icd_oncology_code,
    a.secondary_icd_site_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_abstraction AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
