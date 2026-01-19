-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_appeal_sequence_stg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  patient_dw_id NUMERIC(29) NOT NULL,
  payor_dw_id NUMERIC(29) NOT NULL,
  iplan_insurance_order_num INT64 NOT NULL,
  appeal_num NUMERIC(29) NOT NULL,
  appeal_seq_num INT64 NOT NULL,
  unit_num STRING NOT NULL,
  pat_acct_num NUMERIC(29) NOT NULL,
  iplan_id INT64,
  appeal_seq_begin_bal_amt NUMERIC(32, 3),
  appeal_seq_current_bal_amt NUMERIC(32, 3),
  appeal_seq_end_bal_amt NUMERIC(32, 3),
  appeal_seq_deadline_date DATE,
  appeal_seq_close_date_time DATETIME,
  appeal_seq_root_cause_id NUMERIC(29),
  appeal_seq_root_cause_dtl_text STRING,
  appeal_disp_code_id NUMERIC(29),
  appeal_code_id NUMERIC(29),
  appeal_seq_owner_user_id STRING,
  appeal_seq_create_user_id STRING,
  appeal_seq_create_date_time DATETIME,
  appeal_seq_update_user_id STRING,
  appeal_seq_update_date_time DATETIME,
  appeal_disp_id_update_user_id STRING,
  appeal_disp_id_date_time DATETIME,
  vendor_cd STRING,
  appeal_seq_reopen_user_id STRING,
  appeal_seq_reopen_date_time DATETIME,
  appeal_level_num INT64,
  appeal_sent_date DATE,
  prior_appeal_response_date DATE,
  dw_last_update_date_time DATETIME NOT NULL,
  source_system_code STRING NOT NULL
)
CLUSTER BY patient_dw_id;
