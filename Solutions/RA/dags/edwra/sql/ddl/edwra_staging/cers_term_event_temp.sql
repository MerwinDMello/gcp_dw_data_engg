-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cers_term_event.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cers_term_event_temp
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  user_id NUMERIC(29) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  event_date DATETIME NOT NULL,
  event_type NUMERIC(29),
  show_in_audit NUMERIC(29) NOT NULL,
  is_parent_change NUMERIC(29) NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
