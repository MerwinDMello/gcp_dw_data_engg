CREATE OR REPLACE VIEW {{ params.param_clm_views_dataset_name }}.fact_claim_insurance
AS SELECT
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
  FROM
    {{ params.param_clm_base_views_dataset_name }}.fact_claim_insurance AS a
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.fact_claim AS c ON upper(rtrim(a.claim_id, ' ')) = upper(rtrim(c.claim_id, ' '))
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.facility_dimension AS fd ON rtrim(fd.unit_num, ' ') = rtrim(c.unit_num, ' ')
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.secref_facility AS b ON rtrim(fd.coid, ' ') = rtrim(b.co_id, ' ')
	AND rtrim(fd.company_code, ' ') = rtrim(b.company_code, ' ')
    AND rtrim(b.user_id, ' ') = rtrim(session_user(), ' ')
;
