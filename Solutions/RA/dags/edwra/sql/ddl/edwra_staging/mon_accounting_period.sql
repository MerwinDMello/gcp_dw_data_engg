-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_accounting_period.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  fiscal_year STRING NOT NULL,
  accounting_period NUMERIC(29) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  close_date DATE NOT NULL,
  modified_by NUMERIC(29) NOT NULL,
  modified_date DATE NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
