-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_eom
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description=' Data Warehouse Account Id.\r[F:18-N]'),
  rptg_period STRING NOT NULL OPTIONS(description=' The reporting period for the current end of month data load.'),
  company_code STRING,
  coid STRING OPTIONS(description=' A unique five-character numeric code that identifies a facility.\r[F:5-N]'),
  unit_num STRING OPTIONS(description=' 4-digit corporate assigned number which uniquely identifies each facility.\r[F:4/N]'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description=' Unique identification number assigned by the facility to a patient at admit/registration time.\r[F: 12-N]'),
  admission_date DATE,
  discharge_date DATE,
  final_bill_date DATE OPTIONS(description=' Implementation-defined data'),
  ar_bill_thru_date DATE OPTIONS(description=' Implementation-defined data'),
  billing_status_code STRING OPTIONS(description=' Code indicating the current billing status of the account. Typical values: A - Active/Inhouse; D - Discharged, not billed; B - Billed.'),
  financial_class_code NUMERIC(29) OPTIONS(description=' Code indicating the financial classification of the account.'),
  patient_type_code STRING OPTIONS(description=' Implementation-defined data'),
  account_status_code STRING OPTIONS(description=' Implementation-defined data'),
  total_account_balance_amt NUMERIC(32, 3) OPTIONS(description=' Implementation-defined data'),
  total_billed_charges_amt NUMERIC(32, 3) OPTIONS(description=' Billed Charges|Billed charges is Account Total Charge minus the unbilled charges from the account charge lines.|'),
  total_payment_amt NUMERIC(32, 3) OPTIONS(description=' The sum of payments and unverified collections for the account.'),
  total_adjustment_amt NUMERIC(32, 3) OPTIONS(description=' Total of all accounts receivable transaction for Policy Adjustments.'),
  total_contract_allow_amt NUMERIC(32, 3) OPTIONS(description=' Total of all accounts receivable transaction for Contractual Allowances.'),
  account_id BIGNUMERIC(38) NOT NULL,
  eor_gross_reimbursement_amt NUMERIC(32, 3) OPTIONS(description=' The sum total of all of the individual components of the calculated payment per the Explanation of Reimbursement.'),
  eor_log_date DATE OPTIONS(description=' First date (Month day and year) that this plan information became valid.'),
  rate_schedule_name STRING OPTIONS(description=' Name of the rate schedule, against which the account payer was calculated.'),
  log84_ind STRING OPTIONS(description=' Indicates if a record is flagged for log84 report at month-end.'),
  prior_day_gross_reimb_amt NUMERIC(32, 3),
  source_system_code STRING NOT NULL OPTIONS(description=' Code assigned to Concuity App in EDWRA.\r[F:1-A]'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description=' Date & Timestamp of last record update in EDWRA.\r[')
)
CLUSTER BY patient_dw_id, rptg_period
OPTIONS(
  description='Concuity End Of Month Snapshot'
);
