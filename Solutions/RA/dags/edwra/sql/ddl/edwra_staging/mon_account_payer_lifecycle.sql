-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_lifecycle.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_lifecycle
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_payer_id BIGNUMERIC(38) NOT NULL,
  lifecycle_date DATE NOT NULL,
  lifecycle_event NUMERIC(29) NOT NULL,
  expected_payment NUMERIC(31, 2) NOT NULL,
  actual_payment NUMERIC(31, 2) NOT NULL,
  payer_amount_due NUMERIC(31, 2) NOT NULL,
  mon_status_id NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
