-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_patient_code_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_patient_code_other
   OPTIONS(description='This table is broken down to a seperate table for 837  to hold the Occurrence Codes, Occurrence Spans, Value Codes and Condition Codes.')
  AS SELECT
      rh_837_patient_code_other.claim_id,
      rh_837_patient_code_other.code_type_text,
      rh_837_patient_code_other.code_seq_num,
      rh_837_patient_code_other.patient_dw_id,
      rh_837_patient_code_other.company_code,
      rh_837_patient_code_other.coid,
      rh_837_patient_code_other.pat_acct_num,
      rh_837_patient_code_other.code_value,
      rh_837_patient_code_other.code_amt,
      rh_837_patient_code_other.code_from_date,
      rh_837_patient_code_other.code_to_date,
      rh_837_patient_code_other.source_system_code,
      rh_837_patient_code_other.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_patient_code_other
  ;
