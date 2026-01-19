CREATE OR REPLACE VIEW edwclm_bobj_views.fact_claim_insurance
AS
	SELECT
		fd.company_code,
		fd.coid,
		a.claim_id,
		a.payor_seq_ind,
		a.iplan_id,
		a.payer_id,
		a.payer_sub_id,
		a.payer_name,
		a.health_plan_id,
		a.release_info_cert_desc,
		a.assign_benefit_cert_desc,
		a.prior_pay_amt,
		a.est_due_amt,
		a.other_provider_id,
		a.insured_name,
		a.pat_to_ins_rel_ind,
		a.insured_id,
		a.insured_group_name,
		a.insured_group_num,
		a.treatment_auth_code,
		a.doc_cntrl_num,
		a.employer_name,
		a.dw_last_update_date_time,
		a.source_system_code
  	FROM edwclm_base_views.fact_claim_insurance AS a
   	INNER JOIN edwclm_base_views.fact_claim AS c ON a.claim_id = c.claim_id
   	INNER JOIN edwclm_base_views.facility_dimension AS fd
		ON fd.unit_num = c.unit_num
;
