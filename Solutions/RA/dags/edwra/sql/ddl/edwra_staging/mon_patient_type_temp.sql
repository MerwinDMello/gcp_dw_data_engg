-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_patient_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type_temp
(
  id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  org_id NUMERIC(29) NOT NULL,
  code STRING NOT NULL,
  description STRING NOT NULL,
  ip_op_ind STRING,
  date_created DATE NOT NULL,
  date_updated DATE,
  ext_patient_type STRING,
  ce_patient_type_id NUMERIC(29),
  dw_last_update_date DATETIME
)
CLUSTER BY id, schema_id;
