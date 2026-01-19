-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cd_anch_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.cd_anch_cdm AS SELECT
    cd_anch.cd_anch_sk,
    cd_anch.cd_vld_fr_ts,
    cd_anch.cd_sk,
    cd_anch.cd_ref_type,
    cd_anch.vld_fr_ts,
    cd_anch.vld_to_ts,
    cd_anch.src_sys_ref_cd,
    cd_anch.src_sys_unq_key_txt,
    cd_anch.crt_run_id,
    cd_anch.lst_updt_run_id,
    cd_anch.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.cd_anch
;
