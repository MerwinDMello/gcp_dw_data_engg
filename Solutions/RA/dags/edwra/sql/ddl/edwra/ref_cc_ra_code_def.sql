-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_ra_code_def.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ra_code_def
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  ra_code_def_id NUMERIC(29) NOT NULL,
  ra_code_type STRING,
  ra_code STRING,
  ra_short_desc STRING,
  ra_desc STRING,
  create_date_time DATETIME,
  update_date_time DATETIME,
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY company_code, coid, ra_code_def_id
OPTIONS(
  description='Remittance Code Definition|Remittance Advice group codes, reason codes, status codes, and remark codes.|835 Related'
);
