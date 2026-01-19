-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/customer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.customer
(
  org_id NUMERIC(29) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  name STRING NOT NULL,
  acct_id NUMERIC(29),
  org_status STRING,
  org_role STRING NOT NULL,
  address1 STRING,
  address2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  phone STRING,
  fax STRING,
  billing_address1 STRING,
  billing_address2 STRING,
  billing_city STRING,
  billing_state STRING,
  billing_zip STRING,
  contact_title STRING,
  contact_first_name STRING,
  contact_last_name STRING,
  contact_address1 STRING,
  contact_address2 STRING,
  contact_city STRING,
  contact_state STRING,
  contact_zip STRING,
  contact_phone STRING,
  contact_fax STRING,
  contact_email STRING,
  org_type STRING,
  is_contractor NUMERIC(29),
  is_public NUMERIC(29),
  password_threshold_days NUMERIC(29),
  purchased_products STRING,
  manual_entry_fl NUMERIC(29),
  grant_all_access NUMERIC(29) NOT NULL,
  billing_fax STRING,
  billing_phone STRING,
  allow_edit_patient_data NUMERIC(29) NOT NULL,
  allow_edit_authorization_data NUMERIC(29) NOT NULL,
  allow_acct_email NUMERIC(29) NOT NULL,
  allow_edit_miscellaneous_data NUMERIC(29) NOT NULL,
  dw_last_update_date DATETIME
)
CLUSTER BY org_id, schema_id;
