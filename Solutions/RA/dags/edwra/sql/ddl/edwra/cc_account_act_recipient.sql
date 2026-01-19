-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_account_act_recipient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient
(
  patient_dw_id NUMERIC(29) NOT NULL,
  activity_id BIGNUMERIC(38) NOT NULL OPTIONS(description='The identifier of a specific account activity row.  This has been expanded from 18 to 32 digits as part of the column expansion project.'),
  activity_recipient_id BIGNUMERIC(38) NOT NULL OPTIONS(description='The identifier of a specific account activity recipient row.  This has been expanded from 18 to 32 digits as part of the column expansion project.'),
  company_code STRING NOT NULL,
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  unit_num STRING,
  pat_acct_num NUMERIC(29) NOT NULL,
  recipient_login_userid STRING,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY patient_dw_id;
