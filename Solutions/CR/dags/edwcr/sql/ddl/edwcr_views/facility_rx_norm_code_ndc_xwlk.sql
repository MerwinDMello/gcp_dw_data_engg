-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/facility_rx_norm_code_ndc_xwlk.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.facility_rx_norm_code_ndc_xwlk AS SELECT
    facility_rx_norm_code_ndc_xwlk.company_code,
    facility_rx_norm_code_ndc_xwlk.coid,
    facility_rx_norm_code_ndc_xwlk.rx_norm_mnem_cs,
    facility_rx_norm_code_ndc_xwlk.ndc_num,
    facility_rx_norm_code_ndc_xwlk.ndc_desc,
    facility_rx_norm_code_ndc_xwlk.ndc_qualifier_desc,
    facility_rx_norm_code_ndc_xwlk.ndc_rx_norm_ind,
    facility_rx_norm_code_ndc_xwlk.facility_mnemonic_cs,
    facility_rx_norm_code_ndc_xwlk.network_mnemonic_cs,
    facility_rx_norm_code_ndc_xwlk.source_system_code,
    facility_rx_norm_code_ndc_xwlk.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.facility_rx_norm_code_ndc_xwlk
;
