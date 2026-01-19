CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.phrm_ordr_to_role AS SELECT
    phrm_ordr_to_role.phrm_ordr_sk,
    phrm_ordr_to_role.role_plyr_sk,
    phrm_ordr_to_role.rl_type_ref_cd,
    phrm_ordr_to_role.vld_fr_ts,
    phrm_ordr_to_role.patient_dw_id,
    phrm_ordr_to_role.company_code,
    phrm_ordr_to_role.coid,
    phrm_ordr_to_role.vld_to_ts,
    phrm_ordr_to_role.phrm_ordr_type_ref_cd,
    phrm_ordr_to_role.role_plyr_type_ref_cd,
    phrm_ordr_to_role.effv_fr_dt,
    phrm_ordr_to_role.effv_to_dt,
    phrm_ordr_to_role.subjt_ssukt,
    phrm_ordr_to_role.obj_ssukt,
    phrm_ordr_to_role.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.phrm_ordr_to_role
;
