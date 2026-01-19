-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_payer_contract_eff_term.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer_contract_eff_term
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_payer_id BIGNUMERIC(38) NOT NULL,
  effective_date_start DATE NOT NULL,
  effective_date_end DATE,
  default_contract_code STRING,
  default_contract_name STRING,
  date_updated DATE,
  cers_profile_id NUMERIC(29),
  last_updated_by NUMERIC(29),
  cers_profile_id_rank_2 NUMERIC(29),
  effective_date_start_rank_2 DATE,
  effective_date_end_rank_2 DATE,
  ip_op_cd STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
