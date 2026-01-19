-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ptnt_ob_qry_rslt_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ptnt_ob_qry_rslt_dtl_cdm AS SELECT
    a.ob_qry_sk,
    a.vld_fr_ts,
    a.role_plyr_dw_id,
    a.ptnt_sk,
    a.company_code,
    a.coid,
    a.vld_to_ts,
    a.qry_type_ref_cd,
    a.qry_dspl_ts,
    a.qry_rcrd_ts,
    a.qry_expt_ts,
    a.qry_rslt_shrt_val_txt,
    a.qry_rslt_long_val_txt,
    a.qry_rslt_val_num,
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
