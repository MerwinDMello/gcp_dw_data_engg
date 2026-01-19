-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_patient_diagnosis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_patient_diagnosis
   OPTIONS(description='This table is broken down to a seperate table for 837  to hold the Diagnosis codes.')
  AS SELECT
      rh_837_patient_diagnosis.claim_id,
      rh_837_patient_diagnosis.diag_rank_num,
      rh_837_patient_diagnosis.diag_type_code,
      rh_837_patient_diagnosis.diag_cycle_code,
      rh_837_patient_diagnosis.patient_dw_id,
      rh_837_patient_diagnosis.company_code,
      rh_837_patient_diagnosis.coid,
      rh_837_patient_diagnosis.pat_acct_num,
      rh_837_patient_diagnosis.diag_code,
      rh_837_patient_diagnosis.present_on_admission_ind,
      rh_837_patient_diagnosis.source_system_code,
      rh_837_patient_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_patient_diagnosis
  ;
