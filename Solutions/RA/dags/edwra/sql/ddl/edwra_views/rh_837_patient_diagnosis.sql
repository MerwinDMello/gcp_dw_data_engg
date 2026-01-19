-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/rh_837_patient_diagnosis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.rh_837_patient_diagnosis
   OPTIONS(description='This table is broken down to a seperate table for 837  to hold the Diagnosis codes.')
  AS SELECT
      a.claim_id,
      a.diag_rank_num,
      a.diag_type_code,
      a.diag_cycle_code,
      a.patient_dw_id,
      a.company_code,
      a.coid,
      a.pat_acct_num,
      a.diag_code,
      a.present_on_admission_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_patient_diagnosis AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
