-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_activity_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_activity_type_temp
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  default_completion INT64,
  default_followup_days INT64,
  description STRING,
  name STRING,
  activity_type_id_followup NUMERIC(29),
  incl_on_revenue_recovery_rprt INT64,
  include_in_notes INT64,
  pay_days NUMERIC(29),
  is_create_appeal_or_collection STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
