-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_cers_profile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  cers_profile_id NUMERIC(29) NOT NULL,
  cers_profile_name STRING NOT NULL,
  cers_profile_create_date DATE NOT NULL,
  cers_profile_update_date DATE,
  cers_profile_update_user_nm STRING NOT NULL,
  cers_category_id NUMERIC(29),
  cers_model_ind STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
CLUSTER BY company_code, coid, cers_profile_id;
