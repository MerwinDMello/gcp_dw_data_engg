-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/org.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.org
(
  org_id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  customer_org_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  acct_id NUMERIC(29),
  org_status STRING NOT NULL,
  org_role STRING NOT NULL,
  is_contractor NUMERIC(29),
  is_public NUMERIC(29),
  password_threshold_days NUMERIC(29),
  purchased_products STRING,
  manual_entry_fl NUMERIC(29),
  client_id STRING,
  comments STRING,
  degree_id NUMERIC(29),
  org_type STRING,
  is_practitioner NUMERIC(29),
  market STRING,
  org_affiliation NUMERIC(29),
  pract_first_name STRING,
  pract_last_name STRING,
  pract_middle_name STRING,
  pract_title STRING,
  gender STRING,
  website STRING,
  short_name STRING,
  is_reassignable NUMERIC(29),
  dw_last_update_date DATETIME,
  group_stack STRING
)
CLUSTER BY org_id, schema_id;
