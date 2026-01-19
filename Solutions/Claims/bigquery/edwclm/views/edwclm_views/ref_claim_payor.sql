CREATE OR REPLACE VIEW {{ params.param_clm_views_dataset_name }}.ref_claim_payor
  AS SELECT
    ref_claim_payor.custom_id,
    ref_claim_payor.claim_type_code,
    ref_claim_payor.claim_payor_id,
    ref_claim_payor.claim_type_desc,
    ref_claim_payor.payor_name,
    ref_claim_payor.source_pay_code,
    ref_claim_payor.source_pay_desc,
    ref_claim_payor.dw_last_update_date_time,
    ref_claim_payor.source_system_code
  FROM
    {{ params.param_clm_base_views_dataset_name }}.ref_claim_payor
;
