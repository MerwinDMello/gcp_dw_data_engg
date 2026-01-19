-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_account_payor_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Unique Patient Identifier in EDW Data Warehouse'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Unique Payer Identifier in EDW Data Warehouse'),
  insurance_order_num INT64 NOT NULL OPTIONS(description='Identifies the rank of Payer as Primary, Secondary etc. '),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  acct_payer_issue_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Account Payer Issue Identifier is the identity generated unique identifier for this table.'),
  company_code STRING NOT NULL OPTIONS(description='Company Identifier which identifies HCA and non HCA facilities'),
  unit_num STRING OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='Unique number asssigned to Patient Account'),
  iplan_id INT64 OPTIONS(description='Plan number associated with Insurance'),
  issue_rank_num INT64 OPTIONS(description='Issue Rank indicates the priority (e.g. first, second) of the issue relative to the account payer record'),
  acct_payer_id BIGNUMERIC(38) OPTIONS(description='Account Payer Identifier is the foreign key to the MON_ACCOUNT_PAYER table.'),
  issue_id BIGNUMERIC(38) OPTIONS(description='Issue Identifier represents the foreign key to the Issue reference table.'),
  create_date DATETIME OPTIONS(description='The date upon which the record was created.'),
  create_user_name STRING OPTIONS(description='The user that created the record.'),
  update_date DATETIME OPTIONS(description='The latest date upon which the record was modified.'),
  update_user_name STRING OPTIONS(description='The user who last modified the record.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id, payor_dw_id, insurance_order_num, coid
OPTIONS(
  description='The table Stores Issue reason details and the interface between Concuity and the PATH application keeps the two systems in sync.'
);
