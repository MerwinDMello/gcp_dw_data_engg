-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_preset_value_group_mv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_preset_value_group_mv
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  preset_value_id NUMERIC(29) NOT NULL,
  preset_value_group_type STRING NOT NULL,
  preset_value_display_text STRING NOT NULL,
  dw_last_update_date_time DATETIME,
  source_system_code STRING NOT NULL
)
CLUSTER BY company_code, coid, preset_value_id;
