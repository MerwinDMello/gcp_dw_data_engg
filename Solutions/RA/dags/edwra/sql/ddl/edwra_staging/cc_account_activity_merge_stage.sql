-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_account_activity_merge_stage.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_activity_merge_stage
(
  patient_dw_id NUMERIC(29),
  activity_id BIGNUMERIC(38),
  company_cd STRING,
  coid STRING,
  unit_num STRING,
  payor_dw_id NUMERIC(29),
  iplan_insurance_order_num NUMERIC(29),
  act_pat_acct_num STRING,
  act_iplan_id INT64,
  activity_create_date_time DATETIME,
  activity_update_date_time DATETIME,
  create_login_userid STRING,
  activity_subject_text STRING,
  activity_desc STRING,
  expected_duration_num INT64,
  activity_due_date DATE,
  activity_owner_login_userid STRING,
  create_appeal_or_coll_ind STRING,
  activity_type_id BIGNUMERIC(38),
  activity_status_id NUMERIC(29),
  activity_complete_date_time DATETIME,
  complete_login_userid STRING,
  activity_resolve_text STRING,
  activity_source_code STRING,
  active_ind STRING
)
CLUSTER BY patient_dw_id, activity_id;
