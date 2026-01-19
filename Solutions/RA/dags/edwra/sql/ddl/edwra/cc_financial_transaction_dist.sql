-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_financial_transaction_dist.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction_dist
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL OPTIONS(description=' The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  patient_dw_id NUMERIC(29) NOT NULL,
  account_transaction_id BIGNUMERIC(38) NOT NULL OPTIONS(description=' Column was expanded from 18 to 32 positions.'),
  redistribution_category_id NUMERIC(29) NOT NULL,
  redistribution_date_time DATETIME NOT NULL,
  unit_num STRING,
  pat_acct_num NUMERIC(29),
  redistribution_user_id STRING,
  redistribution_amt NUMERIC(32, 3),
  dw_last_update_date_time DATETIME,
  source_system_code STRING OPTIONS(description=' A one character code indicating the specific source system from which the data originated.')
)
CLUSTER BY patient_dw_id;
