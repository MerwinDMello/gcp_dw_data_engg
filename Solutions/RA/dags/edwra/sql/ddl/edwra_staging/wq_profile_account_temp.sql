-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/wq_profile_account.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.wq_profile_account_temp
(
  schema_id NUMERIC(29) NOT NULL,
  id BIGNUMERIC(38) NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  ssc_code_name STRING NOT NULL,
  wq_profile_id NUMERIC(29) NOT NULL,
  wq_rule_id NUMERIC(29) NOT NULL,
  sort NUMERIC(29),
  mon_account_id BIGNUMERIC(38) NOT NULL,
  mon_account_payer_id BIGNUMERIC(38) NOT NULL,
  user_id NUMERIC(29),
  check_out_status_id NUMERIC(29),
  run_date DATE NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
