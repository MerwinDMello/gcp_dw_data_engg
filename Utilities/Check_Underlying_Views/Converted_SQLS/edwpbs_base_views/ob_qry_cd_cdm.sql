-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ob_qry_cd_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ob_qry_cd_cdm AS SELECT
    a.ob_qry_cd_sk,
    a.vld_fr_ts,
    a.vld_to_ts,
    a.cpn_qry_cd,
    a.cpn_qry_cd_desc,
    a.src_sys_unq_key_txt,
    a.dw_insrt_ts,
    a.crt_run_id,
    a.lst_updt_run_id
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ob_qry_cd AS a
;
