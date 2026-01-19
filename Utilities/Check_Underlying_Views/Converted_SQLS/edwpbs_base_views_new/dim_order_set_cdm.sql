-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_order_set_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_order_set_cdm AS SELECT
    a.vld_to_ts,
    a.ordr_set_sk AS order_set_sk,
    a.vld_fr_ts AS valid_from_date_time,
    a.company_code AS company_code,
    a.coid AS coid,
    a.ordr_set_mnem AS order_set_mnemonic,
    a.ordr_set_desc AS order_set_desc,
    a.ordr_set_orig AS order_set_originating_code,
    a.ebos_flg AS ebos_ind,
    a.pom_lkup_flg AS pom_lookup_ind,
    a.physn_hdg AS physician_hedge_code,
    a.alw_sch_to_ordr_ind AS allow_sch_to_order_ind,
    a.alw_md_ordr_ind AS allow_maryland_order_ind,
    a.ovrd_intrct_ind AS override_interacts_ind,
    a.evdnc_ext_id AS evidence_external_id,
    a.evdnc_updt_ts AS evidence_update_date_time,
    a.evdnc_version_cd AS evidence_version_code,
    a.evdnc_src_orig AS evidence_source_orgn_code,
    a.actv_ind AS active_ind,
    a.src_sys_ref_cd AS source_system_txt,
    a.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ordr_set_dtl AS a
;
