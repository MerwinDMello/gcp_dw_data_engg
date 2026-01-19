CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.fact_svc_line_claim_adj
AS SELECT
		fact_svc_line_claim_adj.patient_remit_sid,
		fact_svc_line_claim_adj.svc_line_seq_num,
		fact_svc_line_claim_adj.line_clm_adj_seq_num,
		fact_svc_line_claim_adj.claim_adj_group_cd,
		fact_svc_line_claim_adj.claim_adj_reason_cd,
		fact_svc_line_claim_adj.adj_amt,
		fact_svc_line_claim_adj.adj_qty_num,
		fact_svc_line_claim_adj.dw_last_update_date_time,
		fact_svc_line_claim_adj.source_system_code,
		fact_svc_line_claim_adj.customer_cd
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.fact_svc_line_claim_adj
;
