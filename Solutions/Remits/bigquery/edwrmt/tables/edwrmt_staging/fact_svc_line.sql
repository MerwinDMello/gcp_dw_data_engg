CREATE TABLE IF NOT EXISTS {{ params.param_rmt_stage_dataset_name }}.fact_svc_line
(
  svc_line_seq_num INT64 NOT NULL,
  patient_remit_sid STRING NOT NULL,
  svc_line_cd STRING,
  service_line_dt DATE,
  service_end_dt DATE,
  serv_line_charge_amt NUMERIC(32, 3),
  serv_line_payment_amt NUMERIC(32, 3),
  rev_code STRING,
  unit_of_svc_paid_cnt NUMERIC(32, 3),
  submitted_service_cd STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  svc_line_cd_qual STRING,
  proc_mod_1 STRING,
  proc_mod_2 STRING,
  proc_mod_3 STRING,
  proc_mod_4 STRING,
  customer_cd STRING
)
CLUSTER BY svc_line_seq_num, patient_remit_sid;
