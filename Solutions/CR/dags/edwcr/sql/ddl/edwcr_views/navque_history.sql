-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/navque_history.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.navque_history AS SELECT
    a.navque_history_id,
    a.navque_action_id,
    a.navque_reason_id,
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
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.navque_history AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
