-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/claim_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.claim_code
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  claim_id BIGNUMERIC(38) NOT NULL,
  code STRING NOT NULL,
  code_date DATE,
  code_type STRING NOT NULL,
  version_id NUMERIC(29) NOT NULL,
  claim_code_rank INT64 NOT NULL,
  modifier_1 STRING,
  modifier_2 STRING,
  date_created DATETIME,
  date_updated DATETIME,
  poa_indicator STRING,
  dx_type_indicator NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
