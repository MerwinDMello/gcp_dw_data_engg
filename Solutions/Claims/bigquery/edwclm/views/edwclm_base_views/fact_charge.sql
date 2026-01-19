CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.fact_charge
AS SELECT
		fact_charge.claim_id,
		fact_charge.charge_seq_num,
		fact_charge.charge_revenue_code,
		fact_charge.charge_ndc_code,
		fact_charge.charge_hcpcs,
		fact_charge.charge_rate_value,
		fact_charge.charge_hcpcs_modifier1_cd,
		fact_charge.charge_hcpcs_modifier2_cd,
		fact_charge.charge_hcpcs_modifier3_cd,
		fact_charge.charge_hcpcs_modifier4_cd,
		fact_charge.charge_service_dt,
		fact_charge.charge_unit_of_svc_num,
		fact_charge.charge_total_amt,
		fact_charge.charge_non_covered_amt,
		fact_charge.dw_last_update_date_time,
		fact_charge.source_system_code,
		fact_charge.charge_ndc_drug_qty,
		fact_charge.charge_ndc_drug_uom
  FROM
    {{ params.param_clm_core_dataset_name }}.fact_charge
;
