CREATE TABLE IF NOT EXISTS {{ params.param_clm_stage_dataset_name }}.fact_code
(
  claim_id STRING NOT NULL,
  code_seq_num INT64 NOT NULL,
  code_type_id STRING NOT NULL,
  code_value STRING NOT NULL,
  code_amt NUMERIC(32, 3),
  code_from_dt DATE,
  code_thru_dt DATE,
  code_poa_ind STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY claim_id, code_seq_num, code_type_id;
