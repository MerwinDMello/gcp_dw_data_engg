CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.facility_rx_norm_code_desc AS SELECT
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
    {{ params.param_auth_base_views_dataset_name }}.facility_rx_norm_code_desc
;
