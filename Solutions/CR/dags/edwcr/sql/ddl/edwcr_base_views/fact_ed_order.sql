CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_ed_order AS SELECT
    fact_ed_order.patient_dw_id,
    fact_ed_order.order_urn,
    fact_ed_order.company_code,
    fact_ed_order.coid,
    fact_ed_order.pat_acct_num,
    fact_ed_order.order_proc_num,
    fact_ed_order.ordering_hcp_dw_id,
    fact_ed_order.order_status_code,
    fact_ed_order.clinical_proc_category_code,
    fact_ed_order.clinical_proc_mnem_cs,
    fact_ed_order.order_date_time,
    fact_ed_order.rx_urn,
    fact_ed_order.order_user_mnemonic_cs,
    fact_ed_order.source_system_code,
    fact_ed_order.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.fact_ed_order
;
