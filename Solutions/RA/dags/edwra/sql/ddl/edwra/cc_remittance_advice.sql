-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_remittance_advice.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice
(
  company_code STRING NOT NULL OPTIONS(description='The company code for a given record.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Used to distinguish one PAYOR from another.'),
  remittance_advice_num INT64 NOT NULL OPTIONS(description='The unique identifier associated with a single remittance.'),
  remittance_header_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Expanded column from 18 to 32 positions.'),
  unit_num STRING OPTIONS(description='A 5 digit code that identifies a specific facility.'),
  iplan_id INT64 OPTIONS(description='An identification code assigned to an insurance plan.  The first three digits identify the payor.  The last two uniquely identify a specific plan belonging to that payor.'),
  payment_date DATE OPTIONS(description='Month, day and year a payment is made that is associated with this RA.'),
  remittance_date DATE OPTIONS(description='Month, day and year a remit is sent.'),
  remittance_amt NUMERIC(32, 3) OPTIONS(description='The amount for an associated remittance.'),
  check_num STRING OPTIONS(description='The check number assiciated with a specific remittance.'),
  icn_num STRING OPTIONS(description='The internal control number for a given remittance.'),
  group_control_num STRING OPTIONS(description='The group control number for a given remittance.'),
  create_date_time DATETIME OPTIONS(description='The creation date and time for a given remittance.'),
  dw_last_update_date_time DATETIME OPTIONS(description='The last update date and time made to the enterprise data warehouse.'),
  source_system_code STRING OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
CLUSTER BY company_code, coid, payor_dw_id, remittance_advice_num
OPTIONS(
  description='This table contains advice related information received on a remittance record.'
);
