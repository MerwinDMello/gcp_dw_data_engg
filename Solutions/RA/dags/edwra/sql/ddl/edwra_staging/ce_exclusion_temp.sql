-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ce_exclusion.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion_temp
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  description STRING NOT NULL,
  is_deleted INT64 DEFAULT 0 NOT NULL,
  user_id_created_by NUMERIC(29) NOT NULL,
  date_created DATE NOT NULL,
  user_id_modified_by NUMERIC(29),
  date_modified DATE,
  dw_last_update_date DATE
)
CLUSTER BY id, schema_id;
