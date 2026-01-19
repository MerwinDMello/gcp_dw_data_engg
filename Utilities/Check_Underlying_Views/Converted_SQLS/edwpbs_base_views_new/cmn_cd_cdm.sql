-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
