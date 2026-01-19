-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/wq_profile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.wq_profile
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  name STRING,
  description STRING,
  is_deleted INT64,
  date_created DATETIME,
  user_id_created_by NUMERIC(29),
  date_modified DATETIME,
  user_id_modified_by NUMERIC(29),
  is_profile_locked INT64,
  lock_update_time DATETIME,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
