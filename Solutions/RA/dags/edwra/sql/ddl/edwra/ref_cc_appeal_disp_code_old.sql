-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_appeal_disp_code_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_disp_code_old
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  apl_disp_code_id NUMERIC(29) NOT NULL,
  apl_disp_code STRING,
  apl_disp_desc STRING,
  apl_disp_status_id NUMERIC(29),
  create_login_userid STRING,
  create_date_time DATETIME,
  update_login_userid STRING,
  update_date_time DATETIME,
  inactive_date DATE,
  active_ind STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING NOT NULL
)
CLUSTER BY company_code, coid, apl_disp_code_id;
