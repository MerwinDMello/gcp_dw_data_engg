CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.srgcl_team_mbr AS SELECT
    srgcl_team_mbr.srg_team_mbr_sk,
    srgcl_team_mbr.vld_fr_ts,
    srgcl_team_mbr.patient_dw_id,
    srgcl_team_mbr.company_code,
    srgcl_team_mbr.coid,
    srgcl_team_mbr.vld_to_ts,
    srgcl_team_mbr.prctnr_sk,
    srgcl_team_mbr.prctnr_role_cd,
    srgcl_team_mbr.prctnr_role_txt,
    srgcl_team_mbr.prctnr_role_mapped_cd,
    srgcl_team_mbr.srg_case_sk,
    srgcl_team_mbr.srg_case_vld_fr_ts,
    srgcl_team_mbr.srg_prcdr_sk,
    srgcl_team_mbr.srg_prcdr_vld_fr_ts,
    srgcl_team_mbr.mbr_desc,
    srgcl_team_mbr.effv_fr_dt,
    srgcl_team_mbr.effv_to_dt,
    srgcl_team_mbr.type_ref_cd,
    srgcl_team_mbr.src_sys_ref_cd,
    srgcl_team_mbr.src_sys_unq_key_txt,
    srgcl_team_mbr.crt_run_id,
    srgcl_team_mbr.lst_updt_run_id,
    srgcl_team_mbr.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.srgcl_team_mbr
;
