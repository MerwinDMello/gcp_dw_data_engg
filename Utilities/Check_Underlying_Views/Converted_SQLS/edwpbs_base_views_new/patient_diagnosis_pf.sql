-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_diagnosis_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_diagnosis_pf AS SELECT
    patient_diagnosis.coid,
    patient_diagnosis.company_code,
    patient_diagnosis.patient_dw_id,
    patient_diagnosis.diag_code,
    patient_diagnosis.diag_cycle_code,
    patient_diagnosis.diag_type_code,
    patient_diagnosis.pat_acct_num,
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
