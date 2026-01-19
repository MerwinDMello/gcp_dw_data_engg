-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_imaging_order.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_imaging_order AS SELECT
    clinical_order_sk,
    order_type_sk,
    patient_sk,
    encounter_sk,
    valid_from_date_time,
    patient_dw_id,
    a.company_code,
    a.coid,
    placer_splmt_serv_info_txt,
    order_mnemonic,
    order_date_time,
    order_source_code,
    order_action_status_code,
    order_type_code,
    order_priority_code,
    accession_txt,
    effective_from_date,
    source_system_original_code,
    source_system_txt,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_imaging_order AS a
    CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b
  WHERE b.co_id = a.coid
   AND b.company_code = a.company_code
   AND b.user_id = session_user()
;
