-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cers_workflow_task.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  ce_workflow_profile_id NUMERIC(29) NOT NULL,
  task_sequence NUMERIC(29) NOT NULL,
  ce_workflow_task_id NUMERIC(29),
  name STRING,
  description STRING,
  expected_duration NUMERIC(29),
  is_attachment_required NUMERIC(29) NOT NULL,
  start_date DATE NOT NULL,
  completion_date DATE,
  is_active NUMERIC(29) NOT NULL,
  comments STRING,
  is_current NUMERIC(29) NOT NULL,
  user_id_created_by NUMERIC(29) NOT NULL,
  date_created DATE NOT NULL,
  user_id_completed_by NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
