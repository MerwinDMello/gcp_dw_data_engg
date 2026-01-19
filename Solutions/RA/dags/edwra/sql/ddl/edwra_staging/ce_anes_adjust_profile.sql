-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ce_anes_adjust_profile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ce_anes_adjust_profile
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  is_deleted NUMERIC(29) NOT NULL,
  user_id_created_by NUMERIC(29),
  date_created DATETIME NOT NULL,
  user_id_modified_by NUMERIC(29),
  date_modified DATETIME,
  is_system_profile NUMERIC(29) NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
