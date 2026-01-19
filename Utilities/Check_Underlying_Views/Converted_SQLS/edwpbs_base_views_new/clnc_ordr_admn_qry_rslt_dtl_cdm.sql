-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clnc_ordr_admn_qry_rslt_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clnc_ordr_admn_qry_rslt_dtl_cdm AS SELECT
    a.clnc_ordr_sk,
    a.vld_fr_ts,
    a.company_code,
    a.coid,
    a.seq_num,
    a.qry_id_txt,
    a.qry_val_txt,
    a.elmnt_rspn_txt,
    a.vld_to_ts,
    a.crt_run_id,
    a.lst_updt_run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.clnc_ordr_admn_qry_rslt_dtl AS a
;
