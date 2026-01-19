-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/claim_physician.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.claim_physician
(
  id BIGNUMERIC(38) NOT NULL,
  claim_id BIGNUMERIC(38) NOT NULL,
  schema_id NUMERIC(29) NOT NULL,
  physician_category NUMERIC(29) NOT NULL,
  first_name STRING,
  last_name STRING,
  middle_name STRING,
  suffix STRING,
  npi STRING,
  other_id_qualifier STRING,
  other_id STRING,
  other_id2_qualifier STRING,
  other_id2 STRING,
  other_id3_qualifier STRING,
  other_id3 STRING,
  other_id4_qualifier STRING,
  other_id4 STRING,
  date_created DATE NOT NULL,
  date_updated DATE NOT NULL,
  is_archived_version STRING NOT NULL,
  date_archived DATE,
  dw_last_update_date DATETIME
)
CLUSTER BY id, claim_id, schema_id;
