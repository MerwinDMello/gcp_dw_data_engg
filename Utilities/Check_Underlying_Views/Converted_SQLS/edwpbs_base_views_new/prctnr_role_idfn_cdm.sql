-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/prctnr_role_idfn_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.prctnr_role_idfn_cdm AS SELECT
    a.role_plyr_sk,
    a.registn_jrsd_cd,
    a.registn_type_ref_cd,
    a.id_txt,
    a.vld_fr_ts,
    a.role_plyr_dw_id,
    a.vld_to_ts,
    a.effv_fr_dt,
    a.effv_to_dt,
    a.parent_src_sys_unq_key_txt,
    a.type_ref_cd,
    a.src_sys_ref_cd,
    a.src_sys_unq_key_txt,
    a.crt_run_id,
    a.lst_updt_run_id,
    a.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.prctnr_role_idfn AS a
;
