-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clnc_ordr_admn_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clnc_ordr_admn_dtl_cdm AS SELECT
    a.clnc_ordr_sk,
    a.vld_fr_ts,
    a.patient_dw_id,
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
