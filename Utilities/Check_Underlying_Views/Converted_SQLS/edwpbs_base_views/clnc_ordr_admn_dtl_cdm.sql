-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clnc_ordr_admn_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clnc_ordr_admn_dtl_cdm AS SELECT
    ROUND(a.clnc_ordr_sk, 0, 'ROUND_HALF_EVEN') AS clnc_ordr_sk,
    a.vld_fr_ts,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.company_code,
    a.coid,
    a.vld_to_ts,
    a.ordr_mnem,
    a.ordr_id,
    a.ordr_ts,
    a.ordr_nm,
    a.ordr_set_mnem,
    a.ordr_src_ref_cd,
    a.ordr_ref_cd_sk,
    a.ordrd_by,
    a.rqst_srvc_ts,
    a.cgy,
    a.ordr_qty,
    a.sts_cd,
    a.dr_ordr_ind,
    a.om_ordr_ind,
    a.clnc_ordr_id,
    a.prn_cmt_txt,
    a.prn_rsn_txt,
    a.effv_fr_dt,
    a.effv_to_dt,
    a.type_ref_cd,
    a.src_sys_unq_key_txt,
    a.src_sys_ref_cd,
    a.lst_updt_run_id,
    a.crt_run_id,
    a.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.clnc_ordr_admn_dtl AS a
;
