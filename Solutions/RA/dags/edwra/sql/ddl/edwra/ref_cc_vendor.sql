-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_vendor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_vendor
(
  company_code STRING NOT NULL OPTIONS(description='Defines the code of the company related to a specific facility.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  vendor_cd STRING NOT NULL OPTIONS(description='The unique alphanumeric code assigned to each vendor.'),
  eff_from_date DATE NOT NULL OPTIONS(description='The date upon which the relationship with the vendor begins.'),
  vendor_name STRING NOT NULL OPTIONS(description='The name of a specific vendor related code.'),
  vendor_desc STRING OPTIONS(description='A description applied to the vendor.'),
  eff_to_date DATE OPTIONS(description='The date upon which the relationship with the vendor ends.'),
  vendor_creation_user_id STRING NOT NULL OPTIONS(description='The vendor record creation user.'),
  vendor_creation_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of the creation of a vendor record.'),
  vendor_modification_user_id STRING OPTIONS(description='The vendor record modification user.'),
  vendor_modification_date_time DATETIME OPTIONS(description='Timestamp of the modification of a vendor record.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
PARTITION BY DATE_TRUNC(eff_from_date, MONTH)
CLUSTER BY company_code, coid, vendor_cd
OPTIONS(
  description='Reference table to all vendors related to an appeal sequence record.'
);
