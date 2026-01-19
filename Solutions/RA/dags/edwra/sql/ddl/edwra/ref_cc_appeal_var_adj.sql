-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_appeal_var_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_var_adj
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  appeal_var_adj_code_id NUMERIC(29) NOT NULL,
  appeal_var_adj_code STRING,
  appeal_var_adj_code_desc STRING,
  appeal_var_adj_eff_end_date DATE,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY company_code, coid, appeal_var_adj_code_id;
