-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/cc_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.cc_eom
(
  schema_id NUMERIC(29) NOT NULL,
  rptg_period STRING NOT NULL,
  coid STRING,
  unit_num STRING,
  pat_acct_num NUMERIC(29),
  admission_date DATE,
  discharge_date DATE,
  final_bill_date DATE,
  ar_bill_thru_date DATE,
  billing_status_code STRING,
  financial_class_code NUMERIC(29),
  patient_type_code STRING,
  account_status_code STRING,
  total_account_balance_amt NUMERIC(32, 3),
  total_billed_charges_amt NUMERIC(32, 3),
  total_payment_amt NUMERIC(32, 3),
  total_adjustment_amt NUMERIC(32, 3),
  total_contract_allow_amt NUMERIC(32, 3),
  account_id BIGNUMERIC(38) NOT NULL,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY rptg_period, coid, pat_acct_num;
