CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.fact_claim_insurance
AS SELECT
		fact_claim_insurance.claim_id,
		fact_claim_insurance.payor_seq_ind,
		fact_claim_insurance.iplan_id,
		fact_claim_insurance.payer_id,
		fact_claim_insurance.payer_sub_id,
		fact_claim_insurance.payer_name,
		fact_claim_insurance.health_plan_id,
		fact_claim_insurance.release_info_cert_desc,
		fact_claim_insurance.assign_benefit_cert_desc,
		fact_claim_insurance.prior_pay_amt,
		fact_claim_insurance.est_due_amt,
		fact_claim_insurance.other_provider_id,
		fact_claim_insurance.insured_name,
		fact_claim_insurance.pat_to_ins_rel_ind,
		fact_claim_insurance.insured_id,
		fact_claim_insurance.insured_group_name,
		fact_claim_insurance.insured_group_num,
		fact_claim_insurance.treatment_auth_code,
		fact_claim_insurance.doc_cntrl_num,
		fact_claim_insurance.employer_name,
		fact_claim_insurance.dw_last_update_date_time,
		fact_claim_insurance.source_system_code
  FROM
    {{ params.param_clm_core_dataset_name }}.fact_claim_insurance
;
