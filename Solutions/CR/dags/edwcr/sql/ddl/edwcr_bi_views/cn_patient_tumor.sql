-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cn_patient_tumor.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cn_patient_tumor AS SELECT
    a.cn_patient_tumor_sid,
    a.referral_source_facility_id,
    a.nav_status_id,
    a.treatment_end_physician_id,
    a.treatment_end_facility_id,
    a.treatment_end_reason_text,
    a.nav_patient_id,
    a.tumor_type_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.electronic_folder_id_text,
    a.identification_period_text,
    a.referral_date,
    a.referring_physician_id,
    a.nav_end_reason_text,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cn_patient_tumor AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
