CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.fact_svc_line_claim_adj
AS SELECT
		a.patient_remit_sid,
		a.svc_line_seq_num,
		a.line_clm_adj_seq_num,
		a.claim_adj_group_cd,
		a.claim_adj_reason_cd,
		a.adj_amt,
		a.adj_qty_num,
		a.dw_last_update_date_time,
		a.source_system_code,
		a.customer_cd
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_svc_line_claim_adj AS a
  WHERE upper(rtrim(a.customer_cd, ' ')) IN(
    'HCA', 'HCAD'
  )
;
