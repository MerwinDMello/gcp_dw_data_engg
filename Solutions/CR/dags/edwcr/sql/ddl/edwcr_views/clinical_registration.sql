-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/clinical_registration.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.clinical_registration AS SELECT
    patient_dw_id,
    fa.company_code,
    fa.coid,
    pat_acct_num,
    patient_market_urn,
    patient_facility_urn,
    patient_facility_urn_abs,
    medical_record_num,
    ethnicity_code,
    registrar_user_mnemonic_cs,
    cancelled_registration_flag,
    mother_patient_facility_urn,
    mother_patient_facility_urn_abs,
    network_mnemonic_cs,
    facility_mnemonic_cs,
    source_system_code,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.clinical_registration AS fa
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON b.co_id = fa.coid
     AND b.company_code = fa.company_code
     AND b.user_id = session_user()
;
