-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/disposition.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.disposition
(
  dispcode INT64 NOT NULL,
  dispdescription STRING NOT NULL,
  dispshortdesc STRING,
  disporder INT64,
  disptype STRING,
  expirationdate DATETIME
)
CLUSTER BY dispcode;
