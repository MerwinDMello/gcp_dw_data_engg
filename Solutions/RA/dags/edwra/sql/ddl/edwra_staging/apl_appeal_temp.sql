-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/apl_appeal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.apl_appeal_temp
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  code STRING,
  description STRING,
  denial_category_id NUMERIC(29) NOT NULL,
  external_code STRING,
  effective_end_date DATE,
  is_user_assignable INT64 NOT NULL,
  is_deleted INT64 NOT NULL,
  user_id_created_by NUMERIC(29) NOT NULL,
  date_created DATE,
  user_id_modified_by NUMERIC(29),
  date_modified DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
