-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_fin_acct_period_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_acct_period_old
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  financial_period_id NUMERIC(29) NOT NULL,
  fiscal_year STRING,
  acctg_period_num INT64,
  period_start_date DATE,
  period_end_date DATE,
  prelim_close_date DATE,
  update_login_userid STRING,
  update_date_time DATETIME,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY company_code, coid, financial_period_id;
