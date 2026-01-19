-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_account_id BIGNUMERIC(38) NOT NULL,
  activity_type_id BIGNUMERIC(38) NOT NULL,
  completion_datetime DATETIME,
  created_datetime DATETIME NOT NULL,
  description STRING,
  due_date DATE,
  mon_account_payer_id BIGNUMERIC(38),
  resolution STRING,
  subject STRING,
  user_id_creator NUMERIC(29),
  change_from_id NUMERIC(29),
  change_to_id NUMERIC(29),
  user_id_activity_owner NUMERIC(29),
  is_deleted INT64,
  status_id NUMERIC(29),
  expected_duration INT64,
  batch_or_thread_id NUMERIC(29),
  activity_order NUMERIC(29),
  user_id_completed_by NUMERIC(29),
  date_exported DATETIME,
  did_it_create_appeal_or_coll STRING,
  date_updated DATETIME,
  mon_payer_id BIGNUMERIC(38),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
