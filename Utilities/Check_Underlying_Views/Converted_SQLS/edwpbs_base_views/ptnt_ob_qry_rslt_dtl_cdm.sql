-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ptnt_ob_qry_rslt_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ptnt_ob_qry_rslt_dtl_cdm AS SELECT
    ROUND(a.ob_qry_sk, 0, 'ROUND_HALF_EVEN') AS ob_qry_sk,
    a.vld_fr_ts,
    ROUND(a.role_plyr_dw_id, 0, 'ROUND_HALF_EVEN') AS role_plyr_dw_id,
    ROUND(a.ptnt_sk, 0, 'ROUND_HALF_EVEN') AS ptnt_sk,
    a.company_code,
    a.coid,
    a.vld_to_ts,
    a.qry_type_ref_cd,
    a.qry_dspl_ts,
    a.qry_rcrd_ts,
    a.qry_expt_ts,
    a.qry_rslt_shrt_val_txt,
    a.qry_rslt_long_val_txt,
    ROUND(a.qry_rslt_val_num, 5, 'ROUND_HALF_EVEN') AS qry_rslt_val_num,
    a.qry_rslt_val_ts,
    a.qry_annt_txt,
    a.user_id_txt,
    a.wan_id_txt,
    a.src_sys_orgnl_cd,
    a.cnfd_cd_sk,
    a.ob_qry_cd_sk,
    a.effv_fr_dt,
    a.effv_to_dt,
    a.type_ref_cd,
    a.src_sys_ref_cd,
    a.src_sys_unq_key_txt,
    a.crt_run_id,
    a.lst_updt_run_id,
    a.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ptnt_ob_qry_rslt_dtl AS a
;
