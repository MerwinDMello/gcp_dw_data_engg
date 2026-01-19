-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/navque_patient_tumor_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.navque_patient_tumor_driver AS SELECT
    a.navque_patient_tumor_driver_sk,
    a.navque_history_id,
    a.navque_action_id,
    a.navque_reason_id,
    a.cancer_patient_driver_sk,
    a.cancer_tumor_driver_sk,
    a.tumor_type_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.message_control_id_text,
    a.message_date,
    a.navque_insert_date,
    a.navque_action_date,
    a.medical_record_num,
    a.patient_market_urn,
    a.network_mnemonic_cs,
    a.transition_of_care_score_num,
    a.navigated_patient_ind,
    a.message_source_flag,
    rna.navque_action_name,
    rna.navque_action_desc,
    rnr.navque_reason_name,
    rnr.navque_reason_desc,
    rn.navigator_name,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.navque_patient_tumor_driver AS a
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_navque_action AS rna ON a.navque_action_id = rna.navque_action_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_navque_reason AS rnr ON a.navque_reason_id = rnr.navque_reason_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_navigator AS rn ON a.navigator_id = rn.navigator_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
