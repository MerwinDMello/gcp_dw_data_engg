-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_mths_medicare.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_mths_medicare AS SELECT
    smry_ar_mths_medicare.coid,
    smry_ar_mths_medicare.company_code,
    smry_ar_mths_medicare.month_id,
    ROUND(smry_ar_mths_medicare.ip_balance_amt, 3, 'ROUND_HALF_EVEN') AS ip_balance_amt,
    ROUND(smry_ar_mths_medicare.op_balance_amt, 3, 'ROUND_HALF_EVEN') AS op_balance_amt,
    ROUND(smry_ar_mths_medicare.op_balance_15_pct_amt, 3, 'ROUND_HALF_EVEN') AS op_balance_15_pct_amt,
    ROUND(smry_ar_mths_medicare.discharge_unbill_amt, 3, 'ROUND_HALF_EVEN') AS discharge_unbill_amt,
    ROUND(smry_ar_mths_medicare.inhouse_unbill_amt, 3, 'ROUND_HALF_EVEN') AS inhouse_unbill_amt,
    ROUND(smry_ar_mths_medicare.unbill_balance_20_pct_amt, 3, 'ROUND_HALF_EVEN') AS unbill_balance_20_pct_amt,
    ROUND(smry_ar_mths_medicare.total_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_balance_amt,
    ROUND(smry_ar_mths_medicare.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
    ROUND(smry_ar_mths_medicare.contractual_amt, 3, 'ROUND_HALF_EVEN') AS contractual_amt,
    ROUND(smry_ar_mths_medicare.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(smry_ar_mths_medicare.ar_days_cnt, 3, 'ROUND_HALF_EVEN') AS ar_days_cnt,
    smry_ar_mths_medicare.dw_last_update_date_time,
    smry_ar_mths_medicare.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_mths_medicare
;
