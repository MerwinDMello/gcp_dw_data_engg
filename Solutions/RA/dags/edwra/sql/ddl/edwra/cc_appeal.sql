-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_appeal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Data warehouse key that identifies a patient.'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Data warehouse key that identifies an insurance payer.'),
  iplan_insurance_order_num INT64 NOT NULL OPTIONS(description='Indicates the precedence of payment liability for insurance plans associated with a patient:  primary, secondary, etc.'),
  appeal_num NUMERIC(29) NOT NULL OPTIONS(description='Indicates the appeal number for a given appeal record.'),
  unit_num STRING OPTIONS(description='4-digit corporate assigned number which uniquely identifies each facility.'),
  pat_acct_num NUMERIC(29) OPTIONS(description='A patient account number which is unique for a specific facility, identifies a given patient.'),
  iplan_id INT64 NOT NULL OPTIONS(description='An identification code assigned to an insurance plan.  The first three digits identify the payor.  The last two uniquely identify a specific plan belonging to that payor.'),
  appeal_amt NUMERIC(32, 3) OPTIONS(description='The total dollar amount associated with a specific appeal.'),
  appeal_origination_date DATE OPTIONS(description='The date when an appeal was opened.'),
  appeal_close_date DATE OPTIONS(description='The date when an appeal was closed.'),
  denied_days_num INT64 OPTIONS(description='The total number of days denied from the payer associated with an appeal record.'),
  appeal_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Unique identifier to the source appeal record. This column was expanded from 18 to 32 positions.'),
  appeal_create_user_id STRING OPTIONS(description='The end-user that created the appeal.'),
  appeal_create_date_time DATETIME OPTIONS(description='The date and time of the appeal creation.'),
  appeal_update_user_id STRING OPTIONS(description='The end-user that updated the appeal.'),
  appeal_update_date_time DATETIME OPTIONS(description='The date and time of the appeal modification.'),
  appeal_reopen_user_id STRING OPTIONS(description='The application user that initiates the reopening of the appeal.'),
  appeal_reopen_date_time DATETIME OPTIONS(description='The date upon which the appeal is reopened for adjustment within the financial accounting period.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Last EDW update date time                               '),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Concuity Appeals, contains account appeal details.'
);
