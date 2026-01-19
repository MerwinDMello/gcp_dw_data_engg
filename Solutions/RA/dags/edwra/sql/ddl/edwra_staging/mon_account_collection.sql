-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_collection
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_payer_id BIGNUMERIC(38),
  amount NUMERIC(32, 3),
  comments STRING,
  mon_account_id BIGNUMERIC(38),
  mon_payer_id BIGNUMERIC(38),
  posting_date DATE,
  verified_as_payment INT64,
  user_id_entered_by NUMERIC(29),
  date_created DATE,
  is_deleted INT64,
  date_entered DATE,
  date_promised DATE,
  date_verified DATE,
  user_id_owner NUMERIC(29),
  date_promised_mod_by NUMERIC(29),
  date_promised_mod_dt DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
