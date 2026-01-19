CREATE OR REPLACE VIEW {{ params.param_clm_views_dataset_name }}.fact_charge
AS SELECT
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
  FROM
    {{ params.param_clm_base_views_dataset_name }}.fact_charge AS a
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.fact_claim AS c ON upper(rtrim(a.claim_id, ' ')) = upper(rtrim(c.claim_id, ' '))
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.facility_dimension AS fd ON rtrim(fd.unit_num, ' ') = rtrim(c.unit_num, ' ')
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.secref_facility AS b ON rtrim(fd.coid, ' ') = rtrim(b.co_id, ' ')
	AND rtrim(fd.company_code, ' ') = rtrim(b.company_code, ' ')
    AND rtrim(b.user_id, ' ') = rtrim(session_user(), ' ')
;
