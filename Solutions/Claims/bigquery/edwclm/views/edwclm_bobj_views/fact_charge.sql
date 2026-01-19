CREATE OR REPLACE VIEW edwclm_bobj_views.fact_charge
AS
	SELECT
		fd.company_code,
		fd.coid,
		a.claim_id,
		a.charge_seq_num,
		a.charge_revenue_code,
		a.charge_ndc_code,
		a.charge_hcpcs,
		a.charge_rate_value,
		a.charge_hcpcs_modifier1_cd,
		a.charge_hcpcs_modifier2_cd,
		a.charge_hcpcs_modifier3_cd,
		a.charge_hcpcs_modifier4_cd,
		a.charge_service_dt,
		a.charge_unit_of_svc_num,
		a.charge_total_amt,
		a.charge_non_covered_amt,
		a.dw_last_update_date_time,
		a.source_system_code,
		a.charge_ndc_drug_qty,
		a.charge_ndc_drug_uom
  	FROM edwclm_base_views.fact_charge AS a
  	INNER JOIN edwclm_base_views.fact_claim AS c
		ON a.claim_id = c.claim_id
	INNER JOIN edwclm_base_views.facility_dimension AS fd
		ON fd.unit_num = c.unit_num
;
