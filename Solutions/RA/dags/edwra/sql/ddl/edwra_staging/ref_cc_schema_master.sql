-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ref_cc_schema_master.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master
(
  schema_id NUMERIC(29) NOT NULL,
  schema_name STRING,
  active_ind STRING,
  create_date_time DATETIME,
  update_date_time DATETIME,
  company_code STRING
)
CLUSTER BY schema_id;
