-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_charge_master.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_charge_master_temp
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  charge_code STRING NOT NULL,
  department STRING,
  description STRING NOT NULL,
  default_amount NUMERIC(31, 2),
  default_hcpcs_code STRING,
  default_revenue_code STRING,
  default_hcpcs_modifier1 STRING,
  default_hcpcs_modifier2 STRING,
  default_hcpcs_modifier3 STRING,
  default_hcpcs_modifier4 STRING,
  date_created DATE NOT NULL,
  date_updated DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
