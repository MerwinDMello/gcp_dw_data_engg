-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Unique data warehouse identifier for a specific patient account.'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Unique data warehouse identifier for a specific insurance payer.'),
  report_end_date DATE NOT NULL OPTIONS(description='The last date of the month for a given report pertaining to the denial end of month data.'),
  iplan_insurance_order_num INT64 NOT NULL OPTIONS(description='Numeric value (1,2,3) that identifies primary, secondary, etc insurance for a given patient at the time of service.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  unit_num STRING NOT NULL OPTIONS(description='The secondary number that identifies a specific facility.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='The patient\'s account number, unique to a facility.'),
  iplan_id INT64 NOT NULL OPTIONS(description='Numeric identifier for a specific insurance company.'),
  denial_status_code STRING OPTIONS(description='Two digit character code that identifies the denial status for a specific claim/appeal.'),
  patient_type_code STRING OPTIONS(description='Code that identifies the status of a patient at time of an encounter (I = Inpatient, O = Outpatient, etc..).'),
  patient_financial_class_code INT64 OPTIONS(description='Numeric identifier that shows the current financial class of a given patient (Medicare, Medicaid, self-pay).'),
  payor_financial_class_code INT64 OPTIONS(description='The financial class for a given payer.'),
  appeal_origination_date DATE OPTIONS(description='The date when the appeal was created for a given denial from insurance.'),
  appeal_level_origination_date DATE OPTIONS(description='The date when the appeal level was created for a given denial from insurance.'),
  disposition_num INT64 OPTIONS(description='The disposition number on a given appeal.'),
  appeal_level_num INT64 OPTIONS(description='The level number for a given appeal as of the end of the month.'),
  beginning_balance_amt NUMERIC(32, 3) OPTIONS(description='The beginning balance for a given denial.'),
  beginning_balance_cnt INT64 OPTIONS(description='The beginning count of denials for a given appeal.'),
  beginning_appeal_amt NUMERIC(32, 3) OPTIONS(description='The beginning appeal amount for a given appeal.'),
  new_denial_account_amt NUMERIC(32, 3) OPTIONS(description='The new calculated denial amount as of the end of the month.'),
  new_denial_account_cnt INT64 OPTIONS(description='The new calculated count of denails for a given account as of the end of the month.'),
  unworked_conversion_amt NUMERIC(32, 3) OPTIONS(description='The conversion amount for an unworked denial as of the end of the month.'),
  unworked_new_accounts_cnt INT64 OPTIONS(description='The number of unworked new denial accounts as of the end of the month.'),
  not_true_denial_amt NUMERIC(32, 3) OPTIONS(description='Dollar amount for a false denial as of the end of the month.'),
  write_off_denial_account_amt NUMERIC(32, 3) OPTIONS(description='Dollar amount for a wirte off denial.'),
  overturned_account_amt NUMERIC(32, 3) OPTIONS(description='Dollar amount for an overturned denied account.'),
  corrections_account_amt NUMERIC(32, 3) OPTIONS(description='Dollar amount for corrections made to a denial.'),
  appeal_closing_date DATE OPTIONS(description='The date when an appeal was closed.'),
  trans_next_party_amt NUMERIC(32, 3) OPTIONS(description='The dollar amount to be paid by the next party.'),
  ending_balance_amt NUMERIC(32, 3) OPTIONS(description='The ending balance amount.'),
  resolved_accounts_cnt INT64 OPTIONS(description='The number of resolved accounts as of the end of the month.'),
  total_charge_amt NUMERIC(32, 3) OPTIONS(description='The total charge amount for denials as of the end of the month.'),
  attending_physician_name_id STRING OPTIONS(description='Identifier for the attending doctor.'),
  account_balance_amt NUMERIC(32, 3) OPTIONS(description='The account balance as of the end of the month.'),
  discharge_date DATE OPTIONS(description='The date when a patient was discharged.'),
  service_code STRING OPTIONS(description='The service code assigned to a given denial as of the end of the month.'),
  medical_record_num STRING OPTIONS(description='The Unique Identifier for a patient within the confines of a Hospital.  It may span multiple encounters.'),
  last_update_hca_3_4_id STRING OPTIONS(description='The HCA employee\'s assigned id or 3-4 for the last worked appeal/denial.'),
  last_update_date DATE OPTIONS(description='The last update date for an appeal/denial as of the end of the month.'),
  work_again_date DATE OPTIONS(description='The next date assigned for when an appeal will be worked again.'),
  appeal_deadline_date DATE OPTIONS(description='The deadline date set for a denial to be worked/closed as of the end of the month.'),
  denied_charges_amt NUMERIC(32, 3) OPTIONS(description='The total charge amount denied by insurance.'),
  cash_adjustment_amt NUMERIC(32, 3) OPTIONS(description='The cash adjustment\'s made to an account as of the end of the month.'),
  ca_adjustment_amt NUMERIC(32, 3) OPTIONS(description='The contractual adjustment amount as of the end of the month.'),
  root_cause STRING OPTIONS(description='The root cause code of a denial.'),
  root_cause_desc STRING OPTIONS(description='The root cause description of a denial.'),
  denial_code_category STRING OPTIONS(description='The category to which a denial belongs to.'),
  disposition_code STRING OPTIONS(description='The disposition code assigned to a specific denial.'),
  appeal_num INT64 OPTIONS(description='The actual number of attempts to clear an appeal denial in concuity'),
  sequence_number INT64 OPTIONS(description='The number of sequences within the concuity denial appeal'),
  appeal_code STRING OPTIONS(description='A unique code assigned to an appeal.'),
  appeal_code_desc STRING OPTIONS(description='The description of the assigned appeal code.'),
  schema_id INT64 OPTIONS(description='The indicator which identifies which database the denial was generated from.  This is used due to multiple databases used with Concuity.'),
  vendor_cd STRING OPTIONS(description='The unique alphanumeric code assigned to each vendor.'),
  source_system_code STRING NOT NULL OPTIONS(description='A once character column that identifies the system source for the data.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='The last insert or update for a given record to the enterprise data warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains denials as of the end of a given month.'
);
