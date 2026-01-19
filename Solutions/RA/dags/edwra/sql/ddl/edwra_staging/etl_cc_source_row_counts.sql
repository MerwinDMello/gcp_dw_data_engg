-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/etl_cc_source_row_counts.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.etl_cc_source_row_counts
(
  concuity_table_name STRING,
  last_analyzed DATETIME,
  num_rows NUMERIC(29),
  status STRING,
  dw_last_update_date DATETIME,
  schema_id NUMERIC(29)
)
CLUSTER BY concuity_table_name;
