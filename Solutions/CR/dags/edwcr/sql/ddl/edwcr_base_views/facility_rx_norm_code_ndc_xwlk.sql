CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.facility_rx_norm_code_ndc_xwlk AS SELECT
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
    {{ params.param_auth_base_views_dataset_name }}.facility_rx_norm_code_ndc_xwlk
;
