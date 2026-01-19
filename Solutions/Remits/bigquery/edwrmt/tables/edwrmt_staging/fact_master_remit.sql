CREATE TABLE IF NOT EXISTS {{ params.param_rmt_stage_dataset_name }}.fact_master_remit
(
  remit_id STRING NOT NULL,
  remit_entered_dt DATE,
  remit_effective_dt DATE,
  remit_total_amt NUMERIC(32, 3),
  check_num STRING,
  payment_type_ind STRING,
  tran_handle_cd STRING,
  credit_debit_ind STRING,
  pay_method_cd STRING,
  pay_fmt_cd STRING,
  sender_remit_id_qual_ind STRING,
  sender_remit_num INT64,
  sender_acct_num STRING,
  sender_acct_num_qual_ind STRING,
  receiver_remit_id_qual_ind STRING,
  receiver_remit_num STRING,
  receiver_acct_num_qual_ind STRING,
  receiver_acct_num STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  customer_cd STRING,
  sender_identifier STRING,
  receiver_identifier STRING,
  interchange_control_num STRING,
  payer_identifier STRING
)
CLUSTER BY remit_id;
