-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/claim_payer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.claim_payer
(
  id BIGNUMERIC(38) NOT NULL,
  claim_id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  payer_rank NUMERIC(29) NOT NULL,
  assign_benefit STRING,
  authorization_code STRING,
  est_amount_due NUMERIC(31, 2),
  group_name STRING,
  insurance_group_no STRING,
  insured_name STRING,
  payer_identification_no STRING,
  payer_name STRING,
  prior_payments NUMERIC(31, 2),
  provider_no STRING,
  rel_to_insured STRING,
  release_info STRING,
  date_created DATE NOT NULL,
  date_updated DATE,
  source_rowid STRING,
  is_billed_to INT64,
  claim_file_ind_code STRING,
  health_plan_id STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY id, claim_id, schema_id;
