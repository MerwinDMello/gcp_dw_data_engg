-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_account_act_recipient_stg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg
(
  patient_dw_id NUMERIC(29) NOT NULL,
  activity_id BIGNUMERIC(38) NOT NULL,
  activity_recipient_id BIGNUMERIC(38) NOT NULL,
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  unit_num STRING,
  pat_acct_num NUMERIC(29) NOT NULL,
  recipient_login_userid STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY patient_dw_id;
