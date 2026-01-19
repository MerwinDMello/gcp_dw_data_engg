CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_diagnosis AS SELECT
    diag_dtl.diag_sk AS diagnosis_sk,
    diag_dtl.encnt_sk AS encounter_sk,
    diag_dtl.cond_type_sk AS conditioning_type_sk,
    diag_dtl.patient_dw_id,
    diag_dtl.company_code,
    diag_dtl.coid,
    coalesce(diag_dtl.diag_id_txt, format('%4d', 0)) AS diagnosis_code,
    diag_dtl.diag_desc AS diagnosis_notes_txt,
    diag_dtl.prty_seq AS priority_sequence,
    diag_dtl.prsnt_on_admt_ind AS present_on_admission_ind,
    diag_dtl.diag_ts AS diagnosis_date_time,
    diag_dtl.src_cds AS source_system_original_code,
    diag_dtl.src_sys_ref_cd AS source_system_txt,
    diag_dtl.dw_insrt_ts AS dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.diag_dtl
  WHERE upper(rtrim(diag_dtl.diag_type_ref_cd)) <> 'DRG'
   AND diag_dtl.vld_to_ts = '9999-12-31 00:00:00'
;
