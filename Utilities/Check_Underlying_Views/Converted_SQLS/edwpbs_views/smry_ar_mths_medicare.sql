-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_mths_medicare.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_mths_medicare AS SELECT
    a.coid,
    a.company_code,
    a.month_id,
    ROUND(a.ip_balance_amt, 3, 'ROUND_HALF_EVEN') AS ip_balance_amt,
    ROUND(a.op_balance_amt, 3, 'ROUND_HALF_EVEN') AS op_balance_amt,
    ROUND(a.op_balance_15_pct_amt, 3, 'ROUND_HALF_EVEN') AS op_balance_15_pct_amt,
    ROUND(a.discharge_unbill_amt, 3, 'ROUND_HALF_EVEN') AS discharge_unbill_amt,
    ROUND(a.inhouse_unbill_amt, 3, 'ROUND_HALF_EVEN') AS inhouse_unbill_amt,
    ROUND(a.unbill_balance_20_pct_amt, 3, 'ROUND_HALF_EVEN') AS unbill_balance_20_pct_amt,
    ROUND(a.total_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_balance_amt,
    ROUND(a.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
    ROUND(a.contractual_amt, 3, 'ROUND_HALF_EVEN') AS contractual_amt,
    ROUND(a.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(a.ar_days_cnt, 3, 'ROUND_HALF_EVEN') AS ar_days_cnt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_mths_medicare AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
