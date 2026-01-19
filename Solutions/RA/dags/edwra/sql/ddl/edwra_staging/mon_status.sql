-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_status
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29),
  coll_probability NUMERIC(31, 2) NOT NULL,
  description STRING,
  mon_stat_category_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  is_priority_status NUMERIC(29) NOT NULL,
  is_available_to_assign NUMERIC(29) NOT NULL,
  can_manually_assign_to_accts NUMERIC(29),
  status_phase NUMERIC(29),
  dscrp_invy_ind INT64,
  denial_invy_ind INT64,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
