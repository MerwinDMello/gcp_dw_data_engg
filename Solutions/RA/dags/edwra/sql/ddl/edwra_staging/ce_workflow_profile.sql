-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ce_workflow_profile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ce_workflow_profile
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  description STRING,
  is_finalized NUMERIC(29),
  finalized_by_user_id NUMERIC(29),
  finalized_on DATE,
  is_deleted NUMERIC(29),
  termination_date DATE,
  user_id_created_by NUMERIC(29) NOT NULL,
  date_created DATE NOT NULL,
  user_id_modified_by NUMERIC(29),
  date_modified DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
