-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_account_payer_calc_excl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_acct_payer_calc_summary_id BIGNUMERIC(38) NOT NULL,
  exclusion_type_id NUMERIC(29) NOT NULL,
  exclusion_identifier STRING NOT NULL,
  exclusion_modifier1 STRING,
  exclusion_modifier2 STRING,
  calculation_method_id NUMERIC(29) NOT NULL,
  service_date DATE,
  quantity NUMERIC(29),
  amount NUMERIC(31, 2),
  charge_amount NUMERIC(31, 2),
  max_unit NUMERIC(29),
  max_unit_scope NUMERIC(29),
  total_amount NUMERIC(31, 2),
  total_quantity NUMERIC(31, 2),
  ce_exclusion_id NUMERIC(29),
  covered_charges NUMERIC(31, 2),
  covered_quantity NUMERIC(31, 2),
  cers_exclusion_id NUMERIC(29),
  ce_service_id BIGNUMERIC(38),
  rate NUMERIC(31, 2),
  creation_date DATETIME,
  creation_user STRING DEFAULT NULL,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
