-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ce_charge_model.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ce_charge_model
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  adjustment_type NUMERIC(29),
  date_created DATETIME NOT NULL,
  date_updated DATETIME,
  user_id_updater NUMERIC(29),
  is_deleted NUMERIC(29),
  is_adjusted_detail_or_master STRING NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
