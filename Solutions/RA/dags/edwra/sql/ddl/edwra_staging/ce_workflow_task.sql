-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ce_workflow_task.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ce_workflow_task
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  ce_workflow_profile_id NUMERIC(29) NOT NULL,
  task_sequence NUMERIC(29) NOT NULL,
  name STRING,
  description STRING,
  expected_duration NUMERIC(29),
  is_attachment_required NUMERIC(29) NOT NULL,
  comments STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
