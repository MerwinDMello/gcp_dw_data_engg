-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_patient_procedure.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_patient_procedure
   OPTIONS(description='This table is broken down to a separate table for 837  to hold the Procedure codes.')
  AS SELECT
      rh_837_patient_procedure.claim_id,
      rh_837_patient_procedure.procedure_seq_num,
      rh_837_patient_procedure.procedure_type_code,
      rh_837_patient_procedure.patient_dw_id,
      rh_837_patient_procedure.company_code,
      rh_837_patient_procedure.coid,
      rh_837_patient_procedure.pat_acct_num,
      rh_837_patient_procedure.procedure_code,
      rh_837_patient_procedure.service_date,
      rh_837_patient_procedure.procedure_rank_num,
      rh_837_patient_procedure.source_system_code,
      rh_837_patient_procedure.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_patient_procedure
  ;
