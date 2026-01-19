-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_eor_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Patient data warehouse identifier.'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Used to distinguish one PAYOR from another.'),
  iplan_insurance_order_num INT64 NOT NULL OPTIONS(description='Indicates the precedence of payment liability for insurance plans associated with a patient:  primary, secondary, etc.'),
  eor_log_date DATE NOT NULL OPTIONS(description='First date (Month day and year) that this plan information became valid.'),
  log_id STRING NOT NULL OPTIONS(description='Code that indicates where to log estimated and actual payment information for the insured\'s associated insurance plan.'),
  log_sequence_num INT64 NOT NULL OPTIONS(description='Sequence number assigned to a record in logging.'),
  eff_from_date DATE NOT NULL OPTIONS(description='Effective date of the calculation.'),
  unit_num STRING NOT NULL OPTIONS(description='The patient accounting concept of identifying a facility.'),
  pat_acct_num NUMERIC(29) OPTIONS(description='Unique account number which identifies a patient.'),
  iplan_id INT64 OPTIONS(description='An identification code assigned to an insurance plan.  The first three digits identify the payor.  The last two uniquely identify a specific plan belonging to that payor.'),
  interest_calc_date DATE OPTIONS(description='Date when interest was calculated.'),
  interest_rate NUMERIC(33, 4) OPTIONS(description='Interest rate that was included as part of the calculation.'),
  interest_days_num INT64 OPTIONS(description='Total number of days included for interest during time of calculation.'),
  interest_amt NUMERIC(32, 3) OPTIONS(description='Calculated interest amount.'),
  interest_stop_date DATE OPTIONS(description='Date when interest was to be excluded from calculation.'),
  first_denial_date DATE OPTIONS(description='Date when reimbursement was first denied.'),
  length_of_stay_days_num INT64 OPTIONS(description='Total number of days for services rendered, total number of days admitted to inpatient stay at a facility.'),
  length_of_service_days_num INT64 OPTIONS(description='Total number of days for services rendered, total number of days admitted to inpatient stay at a facility.'),
  billing_status_code STRING OPTIONS(description='Defined by business rules to determine the status at time of calculation.'),
  calc_lock_ind STRING OPTIONS(description='Indicates if the calculation is locked or unlocked at time of calculation.'),
  calc_success_ind STRING OPTIONS(description='Indicates if the calculation was successful or not.'),
  allow_contract_code_change_ind STRING OPTIONS(description='Indicates if the contract can be changed or not.'),
  payer_eligible_ind STRING OPTIONS(description='Payer eligibility indicator.'),
  owner_override_ind STRING OPTIONS(description='Indicates if the owner of the account can or cannot override the reimbursement calculation.'),
  reason_override_ind STRING OPTIONS(description='Identifies the parent mon reason record at time of calculation.'),
  status_override_ind STRING OPTIONS(description='Identifies the parent status record at time of calculation.'),
  active_ind STRING OPTIONS(description='Identifies if the reimbursement is active during calculation.'),
  calc_base_id NUMERIC(29) OPTIONS(description='Identifies the base of the calculation.'),
  cob_method_id NUMERIC(29) OPTIONS(description='Identifies the coordination of benifits method used at time of calculation.'),
  cers_term_id NUMERIC(29) OPTIONS(description='Identifies the parent rate schedule or insurance contract at time of calculation.'),
  account_payer_status_id NUMERIC(29) OPTIONS(description='Identifies the state of the payer during calculation.'),
  calc_id BIGNUMERIC(38) OPTIONS(description='Identifies the parent latest calculation record.  This was expanded from 18 to 32 positions.'),
  appeal_id BIGNUMERIC(38) OPTIONS(description='Identifies the appeal association at time of calculation. This column was expanded from 12 to 32 positions.'),
  icd_version_desc STRING OPTIONS(description='Descriptive text of the International Classification of Diseases Version'),
  dw_last_update_date_time DATETIME OPTIONS(description='Data waehouse refresh date and time.'),
  source_system_code STRING OPTIONS(description='Indicates the core system from which the data originated. Concuity uses a code of N.')
)
PARTITION BY DATE_TRUNC(eor_log_date, MONTH)
CLUSTER BY company_code, coid, patient_dw_id, payor_dw_id
OPTIONS(
  description='Contains calcultion data related to explanation of reimbursement.'
);
