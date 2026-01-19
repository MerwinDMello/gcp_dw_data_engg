-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_cers_category.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  cers_category_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Primary key for each corresponding EDWRA_STAGING.Ce_rs_Category row.'),
  cers_category_name STRING NOT NULL OPTIONS(description='Calculation engine category descriptive name.'),
  cers_category_deleted_ind STRING OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  cers_category_create_user_id BIGNUMERIC(38) OPTIONS(description='Identifier of the user that created the record.'),
  cers_category_create_date DATE NOT NULL OPTIONS(description='Date the calculation contract category was created.'),
  cers_category_update_user_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Identifier of the user that last modified the record.'),
  cers_category_update_date DATE OPTIONS(description='Date the calculation contract category was last updated.'),
  source_system_code STRING OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME OPTIONS(description='Last update date-time the row in EDWRA was created or updated.')
)
CLUSTER BY company_code, coid, cers_category_id
OPTIONS(
  description='Reference table to calculation engine rate schedule categories.'
);
