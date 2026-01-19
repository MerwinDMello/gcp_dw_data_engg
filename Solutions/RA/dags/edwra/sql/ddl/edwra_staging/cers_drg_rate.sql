-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cers_drg_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cers_drg_rate
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  cers_term_id NUMERIC(29) NOT NULL,
  drg_type STRING NOT NULL,
  drg STRING NOT NULL,
  cost_weight NUMERIC(34, 5),
  geometric_mean NUMERIC(31, 2),
  arithmatic_mean NUMERIC(31, 2),
  marginal_cost_factor NUMERIC(31, 2),
  marginal_cost_factor_day NUMERIC(31, 2),
  fixed_loss_threshold NUMERIC(31, 2),
  transfer_type_id NUMERIC(29),
  add_on_weight NUMERIC(34, 5),
  high_trim_days NUMERIC(29),
  low_trim_days NUMERIC(29),
  date_created DATE,
  date_updated DATE,
  updated_by STRING NOT NULL,
  user_id_updater NUMERIC(29),
  version_id NUMERIC(29),
  base_rate NUMERIC(31, 2),
  fixed_per_diem NUMERIC(31, 2),
  high_trim_modifier NUMERIC(31, 2),
  low_trim_modifier NUMERIC(31, 2),
  high_trim_dollar NUMERIC(31, 2),
  outlier_pct NUMERIC(31, 2),
  capital_add_on NUMERIC(31, 2),
  day_outlier_base NUMERIC(31, 2),
  day_outlier_pct NUMERIC(31, 2),
  low_trim_dollar NUMERIC(31, 2),
  high_trim_per_diem NUMERIC(31, 2),
  low_trim_per_diem NUMERIC(31, 2),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
