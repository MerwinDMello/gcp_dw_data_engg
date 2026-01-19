-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/wq_org_account.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.wq_org_account
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  wq_profile_id NUMERIC(29) NOT NULL,
  wq_rule_id NUMERIC(29) NOT NULL,
  mon_account_id BIGNUMERIC(38) NOT NULL,
  mon_account_payer_id BIGNUMERIC(38) NOT NULL,
  job_no NUMERIC(29) NOT NULL,
  run_date DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id, org_id, mon_account_payer_id;
