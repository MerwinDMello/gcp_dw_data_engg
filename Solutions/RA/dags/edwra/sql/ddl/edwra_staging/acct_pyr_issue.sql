-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/acct_pyr_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.acct_pyr_issue
(
  acct_pyr_issue_id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  acct_pyr_id BIGNUMERIC(38) NOT NULL,
  issue_id BIGNUMERIC(38) NOT NULL,
  issue_rank INT64 NOT NULL,
  creation_dt DATETIME NOT NULL,
  creation_user STRING NOT NULL,
  modification_dt DATETIME,
  modification_user STRING,
  org_id NUMERIC(29) NOT NULL,
  insurance_order_num INT64 NOT NULL,
  pat_acct_num NUMERIC(29) NOT NULL,
  iplan_id INT64,
  dw_last_update_date DATETIME
)
CLUSTER BY acct_pyr_issue_id, schema_id, issue_rank;
