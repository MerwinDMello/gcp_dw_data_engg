CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.prcdr_dtl AS SELECT
    prcdr_dtl.prcdr_sk,
    prcdr_dtl.patient_dw_id,
    prcdr_dtl.company_code,
    prcdr_dtl.coid,
    prcdr_dtl.vld_fr_ts,
    prcdr_dtl.vld_to_ts,
    prcdr_dtl.encnt_sk,
    prcdr_dtl.prty_seq,
    prcdr_dtl.prcdr_cd,  -- originally named prcdr_cd_sk
    prcdr_dtl.prcdr_txt,
    prcdr_dtl.prcdr_ts,
    prcdr_dtl.prcdr_fnctl_type_cd_sk,
    prcdr_dtl.prcdr_prty_cd_sk,
    prcdr_dtl.prcdr_mnt_val,
    prcdr_dtl.ansth_cd_sk,
    prcdr_dtl.ansth_mnt_val,
    prcdr_dtl.cnsnt_cd_sk,
    prcdr_dtl.assoc_diag_cd_sk,
    prcdr_dtl.imag_id_txt,
    --prcdr_dtl.cnfd_cd,  -- not present in upstream view
    prcdr_dtl.dervd_ind,
    prcdr_dtl.effv_fr_dt,
    prcdr_dtl.effv_to_dt,
    prcdr_dtl.type_ref_cd,
    prcdr_dtl.src_sys_orgnl_cd,
    prcdr_dtl.src_sys_ref_cd,
    prcdr_dtl.src_sys_unq_key_txt,
    --prcdr_dtl.crt_run_id,  -- not present in upstream view
    --prcdr_dtl.lst_updt_run_id,  -- not present in upstream view
    prcdr_dtl.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.prcdr_dtl
;
