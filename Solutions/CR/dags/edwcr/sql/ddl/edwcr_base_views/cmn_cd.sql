CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cmn_cd AS SELECT
    cmn_cd.cd_sk,
    cmn_cd.vld_fr_ts,
    cmn_cd.vld_to_ts,
    cmn_cd.cd_val,
    cmn_cd.cd_desc,
    cmn_cd.cd_sys_sk,
    cmn_cd.src_sys_ref_cd,
    cmn_cd.src_sys_unq_key_txt,
    cmn_cd.crt_run_id,
    cmn_cd.lst_updt_run_id,
    cmn_cd.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.cmn_cd
;
