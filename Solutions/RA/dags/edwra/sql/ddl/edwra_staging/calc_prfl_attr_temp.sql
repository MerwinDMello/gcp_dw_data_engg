-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/calc_prfl_attr.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.calc_prfl_attr_temp
(
  calc_prfl_attr_id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  calc_prfl_id NUMERIC(29) NOT NULL,
  excl_method_id NUMERIC(29),
  creation_date DATETIME NOT NULL,
  creation_user STRING NOT NULL,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY calc_prfl_attr_id, schema_id;
