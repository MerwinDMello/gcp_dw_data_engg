-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/org_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.org_type_temp
(
  schema_id NUMERIC(29) NOT NULL,
  org_type STRING NOT NULL,
  org_type_description STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, org_type;
