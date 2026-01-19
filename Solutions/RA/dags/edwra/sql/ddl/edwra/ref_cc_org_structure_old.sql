-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_org_structure_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure_old
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  unit_num STRING NOT NULL,
  schema_id INT64 NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29),
  active_ind STRING,
  schema_name STRING,
  customer_name STRING,
  ssc_name STRING,
  facility_name STRING,
  org_status STRING,
  create_date_time DATETIME,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
CLUSTER BY company_code, coid;
