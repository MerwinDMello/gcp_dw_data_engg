CREATE TABLE IF NOT EXISTS {{ params.param_rmt_stage_dataset_name }}.fact_svc_line_claim_adj
(
  patient_remit_sid STRING NOT NULL,
  svc_line_seq_num INT64 NOT NULL,
  line_clm_adj_seq_num INT64 NOT NULL,
  claim_adj_group_cd STRING,
  claim_adj_reason_cd STRING,
  adj_amt NUMERIC(32, 3),
  adj_qty_num NUMERIC(32, 3),
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  customer_cd STRING
)
CLUSTER BY patient_remit_sid, svc_line_seq_num, line_clm_adj_seq_num;
