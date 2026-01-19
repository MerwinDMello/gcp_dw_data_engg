-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_loa.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_loa_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_id BIGNUMERIC(38) NOT NULL,
  loa_date DATE NOT NULL,
  date_created DATE NOT NULL,
  user_id_created_by NUMERIC(29) NOT NULL,
  reason_code NUMERIC(29),
  loa_source STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
