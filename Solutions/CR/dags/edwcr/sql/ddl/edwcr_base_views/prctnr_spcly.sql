CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.prctnr_spcly AS SELECT
    prctnr_spcly.spcly_cd_sk,
    prctnr_spcly.role_plyr_sk,
    prctnr_spcly.role_plyr_dw_id,
    prctnr_spcly.company_code,
    prctnr_spcly.coid,
    prctnr_spcly.vld_fr_ts,
    prctnr_spcly.vld_to_ts,
    prctnr_spcly.spcly_nm,
    prctnr_spcly.effv_fr_dt,
    prctnr_spcly.effv_to_dt,
    prctnr_spcly.type_ref_cd,
    prctnr_spcly.src_sys_unq_key_txt,
    prctnr_spcly.src_sys_ref_cd,
    prctnr_spcly.crt_run_id,
    prctnr_spcly.lst_updt_run_id,
    prctnr_spcly.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.prctnr_spcly
;
