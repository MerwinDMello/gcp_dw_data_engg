-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cdc_ind.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cdc_ind
(
  schema_id NUMERIC(29) NOT NULL,
  cdc_ind INT64,
  table_name STRING,
  job_name STRING,
  cdc_ind_parm_ind STRING
)
CLUSTER BY schema_id;
