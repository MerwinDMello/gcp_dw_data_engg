-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/sec_establishment_org.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment_org_temp
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  establishment_id NUMERIC(29) NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  eff_dt_begin DATE NOT NULL,
  eff_dt_end DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
