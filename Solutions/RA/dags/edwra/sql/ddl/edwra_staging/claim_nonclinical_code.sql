-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/claim_nonclinical_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.claim_nonclinical_code
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  claim_id BIGNUMERIC(38) NOT NULL,
  code_category NUMERIC(29) NOT NULL,
  payer_rank NUMERIC(29) NOT NULL,
  code STRING NOT NULL,
  amount NUMERIC(31, 2),
  code_date DATE,
  code_date_to DATE,
  date_created DATETIME NOT NULL,
  date_updated DATETIME NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
