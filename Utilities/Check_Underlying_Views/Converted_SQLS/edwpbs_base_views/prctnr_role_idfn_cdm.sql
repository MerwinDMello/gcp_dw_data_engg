-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/prctnr_role_idfn_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.prctnr_role_idfn_cdm AS SELECT
    ROUND(a.role_plyr_sk, 0, 'ROUND_HALF_EVEN') AS role_plyr_sk,
    a.registn_jrsd_cd,
    a.registn_type_ref_cd,
    a.id_txt,
    a.vld_fr_ts,
    ROUND(a.role_plyr_dw_id, 0, 'ROUND_HALF_EVEN') AS role_plyr_dw_id,
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
