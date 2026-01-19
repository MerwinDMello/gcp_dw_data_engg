-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/facility_rx_norm_code_desc.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.facility_rx_norm_code_desc AS SELECT
    facility_rx_norm_code_desc.company_code,
    facility_rx_norm_code_desc.coid,
    facility_rx_norm_code_desc.rx_norm_mnem_cs,
    facility_rx_norm_code_desc.rx_norm_desc_line_num,
    facility_rx_norm_code_desc.rx_norm_desc,
    facility_rx_norm_code_desc.facility_mnemonic_cs,
    facility_rx_norm_code_desc.network_mnemonic_cs,
    facility_rx_norm_code_desc.source_system_code,
    facility_rx_norm_code_desc.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.facility_rx_norm_code_desc
;
