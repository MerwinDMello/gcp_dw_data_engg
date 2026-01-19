-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_eapg_code_def.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_code_def
(
  company_code STRING NOT NULL OPTIONS(description='Identifies the facility code as a 1 byte character. '),
  coid STRING NOT NULL OPTIONS(description='Unique alphanumeric code that identifies a facility.'),
  eapg_code STRING NOT NULL OPTIONS(description='Alphanumeric code assiciated with a specific code type for EAPG marked codes.'),
  eapg_type_code STRING NOT NULL OPTIONS(description='Identifies all codes that are defined as EAPG.  Most values will be literal "EAPG".'),
  eapg_code_version_id BIGNUMERIC(38) OPTIONS(description='EAPG version associated with the record.'),
  eapg_code_name STRING OPTIONS(description='Description of the EAPG code.'),
  eapg_code_short_name STRING OPTIONS(description='Name of the EAPG code.'),
  eapg_code_misc_desc STRING OPTIONS(description='Additional attribute of the code. For DRG types, this is reserved for Medical/Surgical indicator.'),
  eapg_code_misc_char_desc STRING OPTIONS(description='EAPG free form text for the code description.'),
  source_system_code STRING OPTIONS(description='Identifies the source system.  The Concuity product is assigned a code of "N".'),
  dw_last_update_date_time DATETIME OPTIONS(description='Last update datetime of the record in the EDW.')
)
CLUSTER BY company_code, coid, eapg_code, eapg_type_code
OPTIONS(
  description='Reference table to code definitions related to EAPG only.'
);
