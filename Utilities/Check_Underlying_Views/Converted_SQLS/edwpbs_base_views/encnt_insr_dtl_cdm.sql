-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/encnt_insr_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.encnt_insr_dtl_cdm AS SELECT
    ROUND(encnt_insr_dtl.encnt_sk, 0, 'ROUND_HALF_EVEN') AS encnt_sk,
    encnt_insr_dtl.vld_fr_ts,
    encnt_insr_dtl.insr_pln_seq_id,
    encnt_insr_dtl.insr_pln_id,
    ROUND(encnt_insr_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    encnt_insr_dtl.insr_pln_nm,
    encnt_insr_dtl.vld_to_ts,
    encnt_insr_dtl.company_code,
    encnt_insr_dtl.coid,
    encnt_insr_dtl.plcy_nbr,
    encnt_insr_dtl.grp_txt,
    encnt_insr_dtl.grp_nm,
    encnt_insr_dtl.pln_effv_dt,
    encnt_insr_dtl.pln_xprtn_dt,
    encnt_insr_dtl.com_pln_cd,
    encnt_insr_dtl.insr_pln_com_nm,
    encnt_insr_dtl.insr_pln_com_addr_ln1,
    encnt_insr_dtl.insr_pln_com_addr_ln2,
    encnt_insr_dtl.insr_pln_com_addr_city,
    encnt_insr_dtl.insr_pln_com_st,
    encnt_insr_dtl.insr_pln_com_zip_cd,
    encnt_insr_dtl.insr_per_nm,
    encnt_insr_dtl.insr_per_rel_to_ptnt,
    encnt_insr_dtl.insr_per_brth_ts,
    encnt_insr_dtl.insr_per_sex_cd,
    encnt_insr_dtl.insr_pln_com_phn_nbr,
    encnt_insr_dtl.insr_covr_thru,
    encnt_insr_dtl.authrztn_dt,
    encnt_insr_dtl.authrztn_id_txt,
    encnt_insr_dtl.authrztn_src_txt,
    encnt_insr_dtl.authrztn_pcert_rqd_ind,
    encnt_insr_dtl.authrztn_type_txt,
    encnt_insr_dtl.aprvd_len_of_stay,
    encnt_insr_dtl.insr_vrfctn_dt,
    encnt_insr_dtl.insr_vrfctn_ind,
    encnt_insr_dtl.type_ref_cd,
    encnt_insr_dtl.msg_ctrl_id_txt,
    encnt_insr_dtl.src_sys_ref_cd,
    encnt_insr_dtl.src_sys_unq_key_txt,
    encnt_insr_dtl.crt_run_id,
    encnt_insr_dtl.lst_updt_run_id,
    encnt_insr_dtl.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.encnt_insr_dtl
;
