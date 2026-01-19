-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_project_establishment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_project_establishment
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_project_id NUMERIC(29) NOT NULL,
  sec_establishment_id NUMERIC(29) NOT NULL,
  date_created DATETIME NOT NULL,
  user_id_created_by NUMERIC(29) NOT NULL,
  date_updated DATETIME,
  user_id_updater NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
