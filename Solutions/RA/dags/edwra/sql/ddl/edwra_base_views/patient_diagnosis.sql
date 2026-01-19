-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/patient_diagnosis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.patient_diagnosis AS SELECT
    patient_diagnosis.coid,
    patient_diagnosis.company_code,
    patient_diagnosis.pat_acct_num,
    patient_diagnosis.patient_dw_id,
    patient_diagnosis.diag_cycle_code,
    patient_diagnosis.diag_type_code,
    patient_diagnosis.diag_code,
    patient_diagnosis.diag_mapped_code,
    patient_diagnosis.diag_rank_num,
    patient_diagnosis.cc_mcc_category_code,
    patient_diagnosis.diag_affected_drg_code,
    patient_diagnosis.eff_from_date,
    patient_diagnosis.eff_to_date,
    patient_diagnosis.present_on_admission_ind,
    patient_diagnosis.complication_comorbity_ind,
    patient_diagnosis.hosp_acquired_cond_code,
    patient_diagnosis.external_cause_ind,
    patient_diagnosis.pa_last_update_date,
    NULL AS dw_last_update_date_time,
    patient_diagnosis.source_system_code
  FROM
    {{ params.param_auth_base_views_dataset_name }}.patient_diagnosis
;
