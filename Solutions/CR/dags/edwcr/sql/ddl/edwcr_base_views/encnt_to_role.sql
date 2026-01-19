CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.encnt_to_role AS SELECT
    encnt_to_role.encnt_sk,
    encnt_to_role.role_plyr_sk,
    encnt_to_role.rl_type_ref_cd,
    encnt_to_role.vld_fr_ts,
    encnt_to_role.patient_dw_id,
    encnt_to_role.company_code,
    encnt_to_role.coid,
    encnt_to_role.vld_to_ts,
    encnt_to_role.encnt_type_ref_cd,
    encnt_to_role.role_plyr_type_ref_cd,
    encnt_to_role.effv_fr_dt,
    encnt_to_role.effv_to_dt,
    encnt_to_role.subjt_ssukt,
    encnt_to_role.obj_ssukt,
    encnt_to_role.msg_ctrl_id_txt,
    encnt_to_role.crt_run_id,
    encnt_to_role.lst_updt_run_id,
    encnt_to_role.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.encnt_to_role
;
