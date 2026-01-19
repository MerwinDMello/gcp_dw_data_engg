-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_diagnosis_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_diagnosis_pf AS SELECT
    patient_diagnosis.coid,
    patient_diagnosis.company_code,
    ROUND(patient_diagnosis.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    patient_diagnosis.diag_code,
    patient_diagnosis.diag_cycle_code,
    patient_diagnosis.diag_type_code,
    ROUND(patient_diagnosis.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    patient_diagnosis.eff_from_date,
    patient_diagnosis.eff_to_date,
    patient_diagnosis.diag_rank_num,
    patient_diagnosis.present_on_admission_ind,
    patient_diagnosis.complication_comorbity_ind,
    patient_diagnosis.hosp_acquired_cond_code,
    patient_diagnosis.external_cause_ind,
    patient_diagnosis.diag_mapped_code,
    patient_diagnosis.pa_last_update_date,
    patient_diagnosis.cc_mcc_category_code,
    patient_diagnosis.diag_affected_drg_code,
    patient_diagnosis.diag_affected_apr_drg_code,
    patient_diagnosis.source_system_code,
    patient_diagnosis.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.patient_diagnosis
;
