-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/dim_facility.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.dim_facility AS SELECT
    facility_sk,
    fa.company_code,
    fa.coid,
    unit_num,
    facility_name,
    bed_size_cnt,
    prof_service_area_sk,
    active_ind,
    external_ind,
    valid_from_date_time,
    market_sk,
    facility_mnemonic,
    network_mnemonic_cs,
    source_system_txt,
    source_system_code,
    dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.dim_facility AS fa
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON b.co_id = fa.coid
     AND b.company_code = fa.company_code
     AND b.user_id = session_user()
;
