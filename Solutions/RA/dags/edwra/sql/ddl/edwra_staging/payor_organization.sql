-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/payor_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.payor_organization
(
  payor_dw_id NUMERIC(29) NOT NULL,
  coid STRING,
  company_code STRING,
  iplan_id INT64,
  payor_organization_type_code STRING,
  source_system_code STRING
)
CLUSTER BY payor_dw_id;
