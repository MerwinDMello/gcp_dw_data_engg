-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/claim_detail_upload.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.claim_detail_upload
(
  org_id NUMERIC(29),
  schema_id NUMERIC(29) NOT NULL,
  job_no NUMERIC(29) NOT NULL,
  claim_number NUMERIC(29) NOT NULL,
  error_message STRING,
  charge_amount STRING,
  line_no STRING,
  non_covered_amount STRING,
  procedure_code STRING,
  procedure_modifier1 STRING,
  procedure_modifier2 STRING,
  procedure_modifier3 STRING,
  procedure_modifier4 STRING,
  procedure_type STRING,
  quantity STRING,
  rate STRING,
  revenue_code STRING,
  service_date STRING,
  customer_process_no NUMERIC(29),
  claim_type STRING,
  place_of_service STRING,
  epsdt_ind STRING,
  emergency_ind STRING,
  diagnosis_code_pointer_1 NUMERIC(29),
  diagnosis_code_pointer_2 NUMERIC(29),
  diagnosis_code_pointer_3 NUMERIC(29),
  diagnosis_code_pointer_4 NUMERIC(29),
  rendering_provider_id_qual STRING,
  rendering_provider_id STRING,
  rendering_npi_qual STRING,
  rendering_npi STRING,
  assessment_date STRING,
  dw_last_update_date DATETIME
)
CLUSTER BY org_id, schema_id, job_no, claim_number;
