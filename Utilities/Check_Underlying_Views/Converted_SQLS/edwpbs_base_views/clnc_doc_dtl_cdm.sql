-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clnc_doc_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clnc_doc_dtl_cdm AS SELECT
    ROUND(clnc_doc_dtl.clnc_fnd_sk, 0, 'ROUND_HALF_EVEN') AS clnc_fnd_sk,
    clnc_doc_dtl.vld_fr_ts,
    ROUND(clnc_doc_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    clnc_doc_dtl.company_code,
    clnc_doc_dtl.coid,
    clnc_doc_dtl.vld_to_ts,
    clnc_doc_dtl.doc_id_txt,
    clnc_doc_dtl.doc_updtd_ts,
    clnc_doc_dtl.doc_rslt_ts,
    clnc_doc_dtl.doc_rslt_sts_ts,
    clnc_doc_dtl.doc_rslt_sts_ref_cd,
    clnc_doc_dtl.doc_val_txt,
    clnc_doc_dtl.src_sys_orgnl_cd,
    clnc_doc_dtl.cnfd_cd_sk,
    clnc_doc_dtl.doc_type_cd_sk,
    clnc_doc_dtl.effv_fr_dt,
    clnc_doc_dtl.effv_to_dt,
    clnc_doc_dtl.type_ref_cd,
    clnc_doc_dtl.src_sys_ref_cd,
    clnc_doc_dtl.src_sys_unq_key_txt,
    clnc_doc_dtl.crt_run_id,
    clnc_doc_dtl.lst_updt_run_id,
    clnc_doc_dtl.dw_insrt_ts,
    clnc_doc_dtl.msg_ctrl_id_txt
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.clnc_doc_dtl
;
