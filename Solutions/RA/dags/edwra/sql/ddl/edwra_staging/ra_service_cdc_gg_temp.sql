-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ra_service_cdc_gg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ra_service_cdc_gg_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  ra_claim_payment_id BIGNUMERIC(38) NOT NULL,
  procedure_code_type STRING,
  procedure_code STRING,
  procedure_modifier1 STRING,
  procedure_modifier2 STRING,
  procedure_modifier3 STRING,
  procedure_modifier4 STRING,
  procedure_code_description STRING,
  charge_amount NUMERIC(31, 2),
  provider_payment_amount NUMERIC(31, 2),
  revenue_code STRING,
  units_service_paid_count NUMERIC(29),
  submit_code_type STRING,
  submit_procedure_code STRING,
  submit_modifier1 STRING,
  submit_modifier2 STRING,
  submit_modifier3 STRING,
  submit_modifier4 STRING,
  submit_code_description STRING,
  original_units_service_count NUMERIC(29),
  date_created DATE,
  service_end_date DATE,
  service_start_date DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
