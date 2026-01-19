-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_remittance_remark.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_remark
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  patient_dw_id NUMERIC(29) NOT NULL,
  payor_dw_id NUMERIC(29) NOT NULL,
  remittance_advice_num INT64 NOT NULL,
  ra_log_date DATE NOT NULL,
  log_id STRING NOT NULL,
  log_sequence_num INT64 NOT NULL,
  remark_code_type STRING NOT NULL,
  remark_code_seq INT64 NOT NULL,
  unit_num STRING,
  pat_acct_num NUMERIC(29) NOT NULL,
  iplan_insurance_order_num INT64,
  iplan_id INT64,
  remark_code STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY patient_dw_id;
