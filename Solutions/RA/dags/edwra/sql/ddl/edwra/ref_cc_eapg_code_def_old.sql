-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_eapg_code_def_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_code_def_old
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  eapg_code STRING NOT NULL,
  eapg_type_code STRING NOT NULL,
  eapg_code_version_id BIGNUMERIC(38) NOT NULL,
  eapg_code_name STRING,
  eapg_code_short_name STRING,
  eapg_code_misc_desc STRING,
  eapg_code_misc_char_desc STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
CLUSTER BY company_code, coid, eapg_code, eapg_type_code;
