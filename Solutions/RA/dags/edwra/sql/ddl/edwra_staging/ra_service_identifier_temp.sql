-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ra_service_identifier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ra_service_identifier_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  ra_service_id BIGNUMERIC(38) NOT NULL,
  service_sequence NUMERIC(29),
  identifier_type STRING,
  identifier_value STRING,
  date_created DATETIME,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
