-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_appeal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_id BIGNUMERIC(38) NOT NULL,
  mon_payer_id BIGNUMERIC(38) NOT NULL,
  payer_rank NUMERIC(29) NOT NULL,
  appeal_no NUMERIC(29) NOT NULL,
  appeal_close_date DATE,
  date_created DATE NOT NULL,
  user_id_created_by NUMERIC(29),
  date_modified DATE,
  user_id_modified_by NUMERIC(29),
  reopen_date DATETIME,
  reopen_user STRING,
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
