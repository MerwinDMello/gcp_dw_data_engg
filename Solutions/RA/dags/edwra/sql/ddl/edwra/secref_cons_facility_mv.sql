-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/secref_cons_facility_mv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.secref_cons_facility_mv
(
  company_code STRING,
  user_id STRING NOT NULL,
  facility_number STRING
)
CLUSTER BY company_code, user_id, facility_number;
