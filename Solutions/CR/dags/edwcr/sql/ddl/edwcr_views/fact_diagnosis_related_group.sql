-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_diagnosis_related_group.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_diagnosis_related_group AS SELECT
    drg_sk,
    encounter_sk,
    conditioning_type_sk,
    patient_dw_id,
    a.company_code,
    a.coid,
    drg_code,
    drg_notes_txt,
    priority_sequence,
    present_on_admission_ind,
    drg_date,
    source_system_original_code,
    source_system_code,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_diagnosis_related_group AS a
    CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b
  WHERE b.co_id = a.coid
   AND b.company_code = a.company_code
   AND b.user_id = session_user()
;
