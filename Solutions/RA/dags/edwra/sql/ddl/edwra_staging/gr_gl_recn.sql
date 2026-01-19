-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/gr_gl_recn.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.gr_gl_recn
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  cost_report_year_begin DATE NOT NULL,
  pat_acct_num NUMERIC(29) NOT NULL,
  gl_acct_num STRING NOT NULL,
  pat_last_name STRING,
  admission_date DATE,
  discharge_date DATE,
  cost_report_year_end DATE,
  log_type STRING,
  total_gl_posted_acct_amt NUMERIC(32, 3),
  total_ca_log_24_adj_amt NUMERIC(32, 3),
  total_ca_log_65_adj_amt NUMERIC(32, 3),
  dw_last_update_date_time DATETIME NOT NULL,
  source_system_code STRING NOT NULL,
  fourth_day_delay_flag STRING,
  interim_bill_flag STRING,
  log_24_section_name STRING
)

CLUSTER BY company_code, coid, pat_acct_num, gl_acct_num;
