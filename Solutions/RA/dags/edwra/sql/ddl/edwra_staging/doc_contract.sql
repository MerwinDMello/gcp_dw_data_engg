-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/doc_contract.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.doc_contract
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  document_type STRING,
  effective_date DATE,
  previous_contract_id NUMERIC(29),
  original_contract_id NUMERIC(29),
  term NUMERIC(29),
  term_date DATE,
  date_modified DATE,
  date_created DATE,
  is_evergreen NUMERIC(29),
  termination_days NUMERIC(29),
  rate_chg_initial_days NUMERIC(29),
  rate_chg_term_days NUMERIC(29),
  renewal_notice_days NUMERIC(29),
  initial_period_month NUMERIC(29),
  created_by NUMERIC(29) NOT NULL,
  modified_by NUMERIC(29) NOT NULL,
  payer_user_id NUMERIC(29),
  provider_user_id NUMERIC(29),
  status STRING NOT NULL,
  dline_date_rate_change DATE,
  dline_date_eff_term DATE,
  dline_date_termination DATE,
  payer_org_id NUMERIC(29) NOT NULL,
  provider_org_id NUMERIC(29) NOT NULL,
  doc_summary_id NUMERIC(29),
  summary_title STRING,
  anniversary_date DATE,
  anniv_term_days NUMERIC(29),
  with_cause_days NUMERIC(29),
  is_mutual_consent NUMERIC(29),
  contract_name STRING,
  current_anniversary_date DATE,
  current_effective_date DATE,
  current_term_date DATE,
  current_term_days NUMERIC(29),
  custom_value_id01 NUMERIC(29),
  alternate_identifier STRING,
  contract_status NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
