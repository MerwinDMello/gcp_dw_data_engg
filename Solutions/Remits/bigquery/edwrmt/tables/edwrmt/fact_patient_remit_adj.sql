CREATE TABLE IF NOT EXISTS {{ params.param_rmt_core_dataset_name }}.fact_patient_remit_adj
(
  patient_remit_sid STRING NOT NULL,
  adj_seq_num INT64 NOT NULL,
  claim_adj_group_cd STRING,
  claim_adj_reason_cd STRING,
  adjustment_amt NUMERIC(32, 3),
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  adjustment_quantity NUMERIC(32, 3),
  customer_cd STRING,
  PRIMARY KEY (customer_cd, patient_remit_sid, adj_seq_num) NOT ENFORCED
)
CLUSTER BY customer_cd, patient_remit_sid, adj_seq_num;
