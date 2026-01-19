-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/sec_establishment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.sec_establishment
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  level_no NUMERIC(29) NOT NULL,
  level_name STRING NOT NULL,
  establishment_name STRING NOT NULL,
  user_id_created_by NUMERIC(29) NOT NULL,
  date_created DATE NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
