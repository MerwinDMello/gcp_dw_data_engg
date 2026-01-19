-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_financial_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL OPTIONS(description=' The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  patient_dw_id NUMERIC(29) NOT NULL,
  account_transaction_id BIGNUMERIC(38) NOT NULL OPTIONS(description=' This column was expanded from 18 to 32 positions.'),
  unit_num STRING,
  pat_acct_num NUMERIC(29),
  payor_dw_id NUMERIC(29) OPTIONS(description=' Used to distinguish one PAYOR from another.'),
  iplan_insurance_order_num INT64 OPTIONS(description=' Indicates the precedence of payment liability for insurance plans associated with a patient:  primary, secondary, etc.'),
  iplan_id INT64 OPTIONS(description=' An identification code assigned to an insurance plan.  The first three digits identify the payor.  The last two uniquely identify a specific plan belonging to that payor.'),
  transaction_type STRING,
  transaction_enter_date_time DATETIME,
  transaction_eff_date_time DATETIME,
  transaction_code STRING,
  transaction_amt NUMERIC(32, 3),
  transaction_bill_thru_date DATE,
  transaction_comment_text STRING,
  icn_num STRING,
  status_category_id NUMERIC(29),
  reason_id NUMERIC(29),
  financial_period_id NUMERIC(29),
  appeal_num NUMERIC(29),
  appeal_seq_num INT64,
  reversal_ind STRING,
  redistributed_ind STRING,
  parent_account_transaction_id BIGNUMERIC(38) OPTIONS(description=' This column was expanded from 18 to 32 positions.'),
  transaction_create_user_id STRING,
  transaction_create_date_time DATETIME,
  transaction_update_user_id STRING,
  transaction_update_date_time DATETIME,
  dw_last_update_date_time DATETIME NOT NULL,
  source_system_code STRING NOT NULL OPTIONS(description=' A one character code indicating the specific source system from which the data originated.')
)
CLUSTER BY patient_dw_id;
