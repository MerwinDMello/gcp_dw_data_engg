-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_contact.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_contact AS SELECT
    a.cn_patient_contact_sid,
    a.contact_purpose_id,
    a.contact_method_id,
    a.contact_person_id,
    a.nav_patient_id,
    a.tumor_type_id,
    a.diagnosis_result_id,
    a.nav_diagnosis_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.contact_date,
    a.other_purpose_detail_text,
    a.other_person_contacted_text,
    a.time_spent_amount_text,
    a.comment_text,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_contact AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
