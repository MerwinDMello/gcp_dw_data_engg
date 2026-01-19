-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/claim_charge_ub.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.claim_charge_ub
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  claim_id BIGNUMERIC(38) NOT NULL,
  revenue_code STRING NOT NULL,
  form_locator_49 STRING,
  hcpcs_code STRING,
  non_covered_charges NUMERIC(31, 2),
  rate NUMERIC(31, 2),
  service_date DATE,
  total_charges NUMERIC(31, 2) NOT NULL,
  units NUMERIC(29),
  date_created DATETIME NOT NULL,
  date_updated DATETIME,
  line_number NUMERIC(29) NOT NULL,
  source_rowid STRING,
  hcpcs_modifier_1 STRING,
  hcpcs_modifier_2 STRING,
  hcpcs_modifier_3 STRING,
  hcpcs_modifier_4 STRING,
  service_date_to DATE,
  ndc_code STRING,
  unit_of_measure STRING,
  qty BIGNUMERIC(52, 14),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
