-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/apl_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.apl_transaction_temp
(
  id BIGNUMERIC(38) NOT NULL,
  mon_transaction_master_id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  transaction_category_id NUMERIC(29) NOT NULL,
  effective_begin_date DATE NOT NULL,
  effective_end_date DATE NOT NULL,
  date_created DATE NOT NULL,
  date_modified DATE,
  user_id_created_by NUMERIC(29) NOT NULL,
  user_id_modified_by NUMERIC(29),
  dw_last_update_date DATETIME
)

CLUSTER BY mon_transaction_master_id, schema_id;
