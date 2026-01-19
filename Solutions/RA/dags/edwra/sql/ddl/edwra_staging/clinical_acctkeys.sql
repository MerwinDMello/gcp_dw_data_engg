-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/clinical_acctkeys.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys
(
  coid STRING NOT NULL,
  company_code STRING NOT NULL,
  unit_num STRING NOT NULL,
  patient_dw_id NUMERIC(29) NOT NULL,
  pat_acct_num NUMERIC(29) NOT NULL
)
CLUSTER BY patient_dw_id;
