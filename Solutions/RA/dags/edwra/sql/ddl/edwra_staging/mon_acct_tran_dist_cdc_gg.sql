-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_acct_tran_dist_cdc_gg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_acct_tran_dist_cdc_gg
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_transaction_id BIGNUMERIC(38) NOT NULL,
  redistribution_date DATE NOT NULL,
  apl_transaction_category_id NUMERIC(29) NOT NULL,
  category_amount NUMERIC(31, 2) NOT NULL,
  user_id_redistributed_by NUMERIC(29) NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
