-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ra_code_def.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ra_code_def
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  code_type STRING NOT NULL,
  code STRING NOT NULL,
  short_description STRING NOT NULL,
  description STRING NOT NULL,
  date_created DATE,
  date_updated DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
