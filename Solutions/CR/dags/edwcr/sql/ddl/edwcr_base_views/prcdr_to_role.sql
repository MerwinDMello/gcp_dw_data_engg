CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.prcdr_to_role AS SELECT
    prcdr_to_role.prcdr_sk,
    prcdr_to_role.role_plyr_sk,
    prcdr_to_role.rl_type_ref_cd,
    prcdr_to_role.patient_dw_id,
    prcdr_to_role.company_code,
    prcdr_to_role.coid,
    prcdr_to_role.vld_fr_ts,
    prcdr_to_role.vld_to_ts,
    prcdr_to_role.prcdr_type_ref_cd,
    prcdr_to_role.role_type_ref_cd,
    prcdr_to_role.effv_fr_dt,
    prcdr_to_role.effv_to_dt,
    prcdr_to_role.subjt_ssukt,
    prcdr_to_role.obj_ssukt,
    prcdr_to_role.crt_run_id,
    prcdr_to_role.lst_updt_run_id,
    prcdr_to_role.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.prcdr_to_role
;
