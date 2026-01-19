-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity
(
  patient_dw_id NUMERIC(29) NOT NULL,
  activity_id BIGNUMERIC(38) NOT NULL OPTIONS(description=' Identifies a specific account activity row.  This column has expanded from 18 positions to 32.'),
  company_code STRING NOT NULL,
  coid STRING NOT NULL OPTIONS(description=' The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  unit_num STRING,
  payor_dw_id NUMERIC(29) NOT NULL,
  iplan_insurance_order_num INT64 NOT NULL,
  pat_acct_num NUMERIC(29) NOT NULL,
  iplan_id INT64,
  activity_create_date_time DATETIME,
  activity_update_date_time DATETIME,
  create_login_userid STRING,
  activity_subject_text STRING,
  activity_desc STRING,
  expected_duration_num INT64,
  activity_due_date DATE,
  activity_owner_login_userid STRING,
  create_appeal_or_coll_ind STRING,
  activity_type_id NUMERIC(29) NOT NULL,
  activity_status_id NUMERIC(29),
  activity_complete_date_time DATETIME,
  complete_login_userid STRING,
  activity_resolve_text STRING,
  activity_source_code STRING,
  active_ind STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY patient_dw_id;
