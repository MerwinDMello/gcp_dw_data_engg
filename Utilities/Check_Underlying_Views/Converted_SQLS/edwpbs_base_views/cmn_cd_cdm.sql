-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cmn_cd_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.cmn_cd_cdm AS SELECT
    cmn_cd.cd_sk,
    cmn_cd.vld_fr_ts,
    cmn_cd.vld_to_ts,
    cmn_cd.cd_val,
    cmn_cd.cd_desc,
    cmn_cd.cd_sys_sk,
    cmn_cd.src_sys_ref_cd,
    cmn_cd.src_sys_unq_key_txt,
    cmn_cd.crt_run_id,
    cmn_cd.lst_updt_run_id,
    cmn_cd.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.cmn_cd
;
