-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_facility_iplan_contract.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Used to distinguish one PAYOR from another.'),
  cers_profile_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Primary key for each corresponding EDWRA_STAGING.Cers_Profile row.'),
  contract_effective_start_date DATE NOT NULL OPTIONS(description='Date on which the contract mapping starts'),
  patient_type_code STRING NOT NULL OPTIONS(description='Code value used to delineate inpatient (I) and outpatient (O) types.  The addition of this column is to allow for the assignment of a rate schedule profile at this level.'),
  contract_effective_end_date DATE OPTIONS(description='Date on which the contract mapping ends'),
  last_updated_date DATE OPTIONS(description='Date on which the contract mapping was last updated'),
  last_updated_by_uid BIGNUMERIC(38) OPTIONS(description='User identification number of the user that last updated the contract data.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
PARTITION BY DATE_TRUNC(contract_effective_start_date, MONTH)
CLUSTER BY company_code, coid, payor_dw_id, cers_profile_id
OPTIONS(
  description='Data in this table will join Concuity Facility Iplan (Payer) information with Concuity Contract (Rate Schedule Profiles) information.'
);
