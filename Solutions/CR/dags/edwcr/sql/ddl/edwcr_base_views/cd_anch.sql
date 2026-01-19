CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cd_anch AS SELECT
    cd_anch.cd_anch_sk,
    cd_anch.cd_vld_fr_ts,
    cd_anch.cd_sk,
    cd_anch.cd_ref_type,
    cd_anch.vld_fr_ts,
    cd_anch.vld_to_ts,
    cd_anch.src_sys_ref_cd,
    cd_anch.src_sys_unq_key_txt,
    cd_anch.crt_run_id,
    cd_anch.lst_updt_run_id,
    cd_anch.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.cd_anch
;
