-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/clnc_ordr_admn_qry_rslt_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.clnc_ordr_admn_qry_rslt_dtl_cdm AS SELECT
    ROUND(a.clnc_ordr_sk, 0, 'ROUND_HALF_EVEN') AS clnc_ordr_sk,
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
