CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.phrm_ordr_dtl AS SELECT
    phrm_ordr_dtl.phrm_ordr_sk,
    phrm_ordr_dtl.vld_fr_ts,
    phrm_ordr_dtl.patient_dw_id,
    phrm_ordr_dtl.company_code,
    phrm_ordr_dtl.coid,
    phrm_ordr_dtl.vld_to_ts,
    phrm_ordr_dtl.subst_cd,  -- was originally named subst_cd_sk
    phrm_ordr_dtl.fill_ordr_txt,
    phrm_ordr_dtl.plcr_ordr_txt,
    phrm_ordr_dtl.plcr_grp_txt,
    phrm_ordr_dtl.accsn_txt,
    phrm_ordr_dtl.parnt_ordr_txt,
    phrm_ordr_dtl.ordr_actn_sts_ref_cd,
    phrm_ordr_dtl.ordr_actn_rsn_ref_cd,
    phrm_ordr_dtl.ordr_sts_ref_cd,
    phrm_ordr_dtl.ordr_sts_modfr_ref_cd,
    phrm_ordr_dtl.ordr_cgy_cd,  -- was originally named ordr_cgy_cd_sk
    phrm_ordr_dtl.ordr_nm,
    phrm_ordr_dtl.ordr_set_mnem,
    phrm_ordr_dtl.ordr_mnem,
    phrm_ordr_dtl.alw_subst_ind,
    phrm_ordr_dtl.drug_cmpnd_ind,
    phrm_ordr_dtl.prmit_num_refills_qty,
    phrm_ordr_dtl.hmn_rvw_need_ind,
    phrm_ordr_dtl.entr_authrztn_mode_ref_cd,
    phrm_ordr_dtl.entr_org_nm,
    phrm_ordr_dtl.entr_dvc,
    phrm_ordr_dtl.fill_expct_avail_ts,
    phrm_ordr_dtl.adv_bene_notc_ref_cd,
    phrm_ordr_dtl.adv_bene_notc_ovrd_rsn_ref_cd,
    -- phrm_ordr_dtl.cnfd_cd_sk,  -- column doesn't exist in upstream views
    phrm_ordr_dtl.rspn_flg,
    phrm_ordr_dtl.call_bck_ph_txt,
    -- phrm_ordr_dtl.diag_sk,  -- column doesn't exist in upstream views
    phrm_ordr_dtl.effv_fr_dt,
    phrm_ordr_dtl.effv_to_dt,
    phrm_ordr_dtl.src_sys_orgnl_cd,
    phrm_ordr_dtl.type_ref_cd,
    phrm_ordr_dtl.src_sys_ref_cd,
    phrm_ordr_dtl.src_sys_unq_key_txt,
    phrm_ordr_dtl.clnc_ordr_admn_sk,
    phrm_ordr_dtl.clnc_ordr_admn_ssukt,
    -- phrm_ordr_dtl.crt_run_id,  -- column doesn't exist in upstream views
    phrm_ordr_dtl.msg_ctrl_id_txt,
    -- phrm_ordr_dtl.lst_updt_run_id,  -- column doesn't exist in upstream views
    phrm_ordr_dtl.dw_insrt_ts
  FROM
    {{ params.param_auth_base_views_dataset_name }}.phrm_ordr_dtl
;
