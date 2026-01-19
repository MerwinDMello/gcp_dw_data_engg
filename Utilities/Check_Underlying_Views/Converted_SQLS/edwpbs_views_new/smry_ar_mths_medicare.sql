-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_mths_medicare.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_mths_medicare AS SELECT
    a.coid,
    a.company_code,
    a.month_id,
    a.ip_balance_amt,
    a.op_balance_amt,
    a.op_balance_15_pct_amt,
    a.discharge_unbill_amt,
    a.inhouse_unbill_amt,
    a.unbill_balance_20_pct_amt,
    a.total_balance_amt,
    a.gross_revenue_amt,
    a.contractual_amt,
    a.net_revenue_amt,
    a.ar_days_cnt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_mths_medicare AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
;
