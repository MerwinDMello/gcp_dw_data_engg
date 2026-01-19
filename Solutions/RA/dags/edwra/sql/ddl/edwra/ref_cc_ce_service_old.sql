-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_ce_service_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ce_service_old
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  ce_service_id BIGNUMERIC(38) NOT NULL,
  ce_service_name STRING,
  doc_service_ind STRING,
  pass_through_ind STRING,
  pass_through_active_ind STRING,
  ce_service_create_date_time DATETIME,
  ce_service_update_date_time DATETIME,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
CLUSTER BY company_code, coid, ce_service_id;
