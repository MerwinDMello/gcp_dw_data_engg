-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_facility_iplan.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan
(
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Used to distinguish one PAYOR from another.'),
  company_code STRING OPTIONS(description='A one character field used to identify the company which owns a facility.'),
  coid STRING OPTIONS(description='A five character field used to identify the company which owns a facility.'),
  unit_num STRING OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  payer_id NUMERIC(29) NOT NULL OPTIONS(description='eHC\'s ID of the payer.'),
  payer_name STRING OPTIONS(description='Name of the payer.'),
  financial_class_code NUMERIC(29) OPTIONS(description='Financial Class Code of the Payer.'),
  iplan_id INT64 OPTIONS(description='The customer organization\'s code representing the payer.'),
  payer_part_b_ind STRING OPTIONS(description='Implementation-defined data'),
  model_covered_population_text STRING OPTIONS(description='Covered population to be used by modeling.'),
  model_product_class_text STRING OPTIONS(description='Product class to be used by Modeling.'),
  create_date_time DATETIME OPTIONS(description='Date of record creation.'),
  update_date_time DATETIME OPTIONS(description='Date of last record update.'),
  icd10_conversion_date DATE OPTIONS(description='Date the Insurance Plan Provider converted to the new ICD10 coding.'),
  ip_provider_num STRING OPTIONS(description='The financially responsible party identifier of the provider for Inpatients.  '),
  op_provider_num STRING OPTIONS(description='The financially responsible party identifier of the provider for Outpatients.  '),
  dw_last_update_date_time DATETIME OPTIONS(description='Last update made to the data warehouse.'),
  source_system_code STRING OPTIONS(description='Source system for the record.')
)
CLUSTER BY payor_dw_id
OPTIONS(
  description='An organization that is financially responsible for paying healthcare claims. The person receiving the medical care is usually a member or subscriber (or related to one) of the organization.'
);
