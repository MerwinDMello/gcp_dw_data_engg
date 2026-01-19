CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_network_to_oid AS SELECT
    ref_network_to_oid.network_oid,
    ref_network_to_oid.network_mnemonic_cs,
    ref_network_to_oid.source_system_code,
    ref_network_to_oid.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_network_to_oid
;
