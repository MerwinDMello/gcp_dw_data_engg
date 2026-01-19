-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ce_patient_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ce_patient_type_temp
(
  schema_id NUMERIC(29) NOT NULL,
  id NUMERIC(29) NOT NULL,
  customer_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  ip_op_ind STRING NOT NULL,
  date_created DATETIME NOT NULL,
  date_updated DATETIME,
  updated_by STRING NOT NULL,
  user_id_updater NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY schema_id, id;
