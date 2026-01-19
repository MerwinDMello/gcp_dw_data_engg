-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_account_payer_stts_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_account_payer_stts_old
(
  company_code STRING NOT NULL,
  status_id NUMERIC(29) NOT NULL,
  status_category_id NUMERIC(29) NOT NULL,
  status_name STRING,
  status_desc STRING,
  status_phase_id NUMERIC(29),
  probability_pct NUMERIC(33, 4),
  incl_new_acct_ind STRING,
  pvnt_acct_ovrd_ind STRING,
  acct_level_assn_ind STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY company_code, status_id;
