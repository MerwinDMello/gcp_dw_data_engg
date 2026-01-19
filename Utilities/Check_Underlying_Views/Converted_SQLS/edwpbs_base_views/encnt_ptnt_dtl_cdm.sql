-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/encnt_ptnt_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.encnt_ptnt_dtl_cdm AS SELECT
    ROUND(encnt_ptnt_dtl.encnt_sk, 0, 'ROUND_HALF_EVEN') AS encnt_sk,
    encnt_ptnt_dtl.vld_fr_ts,
    ROUND(encnt_ptnt_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    encnt_ptnt_dtl.vld_to_ts,
    encnt_ptnt_dtl.company_code,
    encnt_ptnt_dtl.coid,
    encnt_ptnt_dtl.registn_ts,
    encnt_ptnt_dtl.brth_ts,
    encnt_ptnt_dtl.fl_nm,
    encnt_ptnt_dtl.prfx_nm,
    encnt_ptnt_dtl.frst_nm,
    encnt_ptnt_dtl.mid_nm,
    encnt_ptnt_dtl.lst_nm,
    encnt_ptnt_dtl.sfx_nm,
    encnt_ptnt_dtl.mthr_maid_nm,
    encnt_ptnt_dtl.umpi_txt,
    encnt_ptnt_dtl.sex_ref_cd,
    encnt_ptnt_dtl.relg_cd_sk,
    encnt_ptnt_dtl.mrtl_sts_ref_cd,
    encnt_ptnt_dtl.lang_cd_sk,
    encnt_ptnt_dtl.race_cd_sk,
    encnt_ptnt_dtl.ethncty_cd_sk,
    encnt_ptnt_dtl.vip_ind,
    encnt_ptnt_dtl.type_ref_cd,
    encnt_ptnt_dtl.src_sys_ref_cd,
    encnt_ptnt_dtl.src_sys_unq_key_txt,
    encnt_ptnt_dtl.msg_ctrl_id_txt,
    encnt_ptnt_dtl.crt_run_id,
    encnt_ptnt_dtl.lst_updt_run_id,
    encnt_ptnt_dtl.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.encnt_ptnt_dtl
;
