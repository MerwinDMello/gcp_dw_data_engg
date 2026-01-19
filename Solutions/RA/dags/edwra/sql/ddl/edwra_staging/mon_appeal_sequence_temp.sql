-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/mon_appeal_sequence.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence_temp
(
  id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  mon_appeal_id BIGNUMERIC(38) NOT NULL,
  sequence_no NUMERIC(29) NOT NULL,
  appeal_balance_amt_begin NUMERIC(31, 2) NOT NULL,
  appeal_balance_end_amt NUMERIC(31, 2),
  apl_root_cause_id NUMERIC(29),
  root_cause_detail STRING,
  apl_appeal_id NUMERIC(29),
  apl_disposition_id NUMERIC(29),
  deadline_date DATE,
  sequence_owner_id NUMERIC(29) NOT NULL,
  appealed_amt NUMERIC(31, 2),
  appeal_balance_amt NUMERIC(31, 2) NOT NULL,
  sequence_close_date DATE,
  user_id_created_by NUMERIC(29),
  date_created DATE,
  user_id_modified_by NUMERIC(29),
  date_modified DATE,
  disposition_code_modified_date DATE,
  disposition_code_modified_by NUMERIC(29),
  reopen_date DATE,
  reopen_user STRING,
  vendor_id NUMERIC(29),
  creation_date DATETIME,
  creation_user STRING,
  modification_date DATETIME,
  modification_user STRING,
  dw_last_update_date DATETIME,
  apl_lvl NUMERIC(29),
  apl_sent_dt DATE,
  prior_apl_rspn_dt DATE
)
CLUSTER BY id, schema_id, mon_appeal_id, sequence_no;
