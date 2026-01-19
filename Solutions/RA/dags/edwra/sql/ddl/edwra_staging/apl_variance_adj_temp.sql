-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/apl_variance_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.apl_variance_adj_temp
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  code STRING NOT NULL,
  description STRING,
  date_created DATETIME NOT NULL,
  date_modified DATETIME,
  effective_end_date DATE NOT NULL,
  user_id_created_by NUMERIC(29),
  user_id_modified_by NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
