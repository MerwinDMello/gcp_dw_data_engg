-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_account_occur_span_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_account_occur_span_code
(
  patient_dw_id NUMERIC(29) NOT NULL,
  occur_span_code_seq INT64 NOT NULL,
  company_code STRING,
  coid STRING,
  unit_num STRING,
  pat_acct_num NUMERIC(29),
  occur_span_code STRING,
  occur_span_from_date DATE,
  occur_span_to_date DATE,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY patient_dw_id;
