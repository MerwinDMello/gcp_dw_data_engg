CREATE TABLE IF NOT EXISTS {{ params.param_rmt_core_dataset_name }}.fact_patient_qual
(
  qual_seq_num INT64 NOT NULL,
  patient_remit_sid STRING NOT NULL,
  svc_line_seq_num INT64,
  qualifier_code STRING,
  pat_qual_id STRING,
  pat_qual_ind STRING,
  pat_qual_name STRING,
  pat_qual_amt NUMERIC(32, 3),
  pat_qual_code STRING,
  pat_qual_cnt INT64,
  dw_last_update_date_time DATETIME,
  source_system_code STRING,
  customer_cd STRING,
  PRIMARY KEY (qual_seq_num, patient_remit_sid, svc_line_seq_num) NOT ENFORCED
)
CLUSTER BY qual_seq_num, patient_remit_sid, svc_line_seq_num;
