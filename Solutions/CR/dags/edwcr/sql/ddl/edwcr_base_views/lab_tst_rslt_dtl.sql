CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.lab_tst_rslt_dtl AS SELECT
  lab_tst_rslt_dtl.clnc_fnd_sk,
  lab_tst_rslt_dtl.vld_fr_ts,
  lab_tst_rslt_dtl.patient_dw_id,
  lab_tst_rslt_dtl.company_code,
  lab_tst_rslt_dtl.coid,
  lab_tst_rslt_dtl.pt_of_care_lo_cd,
  lab_tst_rslt_dtl.pt_of_care_rm_cd,
  lab_tst_rslt_dtl.pt_of_care_bed_cd,
  lab_tst_rslt_dtl.pt_of_care_lo_sk,
  lab_tst_rslt_dtl.vld_to_ts,
  lab_tst_rslt_dtl.clnc_ordr_sk,
  lab_tst_rslt_dtl.lab_tst_type_ref_cd,
  lab_tst_rslt_dtl.lab_tst_id_txt,
  lab_tst_rslt_dtl.lab_tst_subid_txt,
  lab_tst_rslt_dtl.lab_tst_coll_ts,
  lab_tst_rslt_dtl.lab_tst_spcmn_rcvd_ts,
  lab_tst_rslt_dtl.lab_tst_rptd_ts,
  lab_tst_rslt_dtl.lab_tst_rslt_sts_ts,
  lab_tst_rslt_dtl.lab_tst_rslt_sts_ref_cd,
  lab_tst_rslt_dtl.lab_tst_val_nmrc_ind,
  lab_tst_rslt_dtl.lab_tst_val_unt_type_cd,
  lab_tst_rslt_dtl.lab_tst_val_txt,
  lab_tst_rslt_dtl.lab_tst_val_num,
  lab_tst_rslt_dtl.ref_rng_low,
  lab_tst_rslt_dtl.ref_rng_hi,
  lab_tst_rslt_dtl.ref_rng_txt,
  lab_tst_rslt_dtl.ntrt_of_abnrml_tst_ref_cd,
  lab_tst_rslt_dtl.obsrv_methd_cd,
  lab_tst_rslt_dtl.src_sys_orgnl_cd,
  -- lab_tst_rslt_dtl.cnfd_cd_sk,  -- column doesn't exist in upstream views
  lab_tst_rslt_dtl.abnrml_flg_nm,
  lab_tst_rslt_dtl.lab_tst_cd,  -- was originally named lab_tst_cd_sk
  -- lab_tst_rslt_dtl.microorganism_cd_sk,  -- column doesn't exist in upstream views
  lab_tst_rslt_dtl.effv_fr_dt,
  -- lab_tst_rslt_dtl.effv_to_dt,  -- column doesn't exist in upstream views
  lab_tst_rslt_dtl.type_ref_cd,
  lab_tst_rslt_dtl.src_sys_ref_cd,
  lab_tst_rslt_dtl.src_sys_unq_key_txt,
  lab_tst_rslt_dtl.prfm_org,
  lab_tst_rslt_dtl.msg_ctrl_id_txt,
  -- lab_tst_rslt_dtl.crt_run_id,  -- column doesn't exist in upstream views
  -- lab_tst_rslt_dtl.lst_updt_run_id,  -- column doesn't exist in upstream views
  lab_tst_rslt_dtl.dw_insrt_ts
FROM {{ params.param_auth_base_views_dataset_name }}.lab_tst_rslt_dtl;
