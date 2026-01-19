-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/facility_rx_norm_code.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.facility_rx_norm_code AS SELECT
    facility_rx_norm_code.company_code,
    facility_rx_norm_code.coid,
    facility_rx_norm_code.rx_norm_mnem_cs,
    facility_rx_norm_code.rx_norm_active_ind,
    facility_rx_norm_code.rx_norm_external_mnemonic,
    facility_rx_norm_code.rx_norm_last_update,
    facility_rx_norm_code.rx_norm_replaced_by_mnem_cs,
    facility_rx_norm_code.rx_norm_terminology_desc,
    facility_rx_norm_code.rx_norm_type_code,
    facility_rx_norm_code.facility_mnemonic_cs,
    facility_rx_norm_code.network_mnemonic_cs,
    facility_rx_norm_code.source_system_code,
    facility_rx_norm_code.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.facility_rx_norm_code
;
