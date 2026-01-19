-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cers_workflow_task_owner.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task_owner
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  cers_workflow_task_id NUMERIC(29) NOT NULL,
  user_id_owner NUMERIC(29),
  owner_name STRING,
  owner_email STRING,
  is_required_to_complete NUMERIC(29) NOT NULL,
  is_notified NUMERIC(29) NOT NULL,
  completion_date DATE,
  checked_off_by_user_id NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
