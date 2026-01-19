-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/vendor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.vendor_temp
(
  vendor_id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  cust_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  code STRING,
  description STRING,
  eff_date DATETIME,
  expr_date DATETIME,
  creation_date DATETIME NOT NULL,
  creation_user STRING NOT NULL,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY vendor_id, schema_id;
