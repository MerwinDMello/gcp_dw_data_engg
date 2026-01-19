-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_root_cause.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_root_cause
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  root_cause_id NUMERIC(29) NOT NULL,
  root_cause_code STRING NOT NULL,
  root_cause_desc STRING,
  create_login_userid STRING,
  create_date_time DATETIME,
  update_login_userid STRING,
  update_date_time DATETIME,
  inactive_date DATE,
  active_ind STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
CLUSTER BY company_code, coid, root_cause_id;
