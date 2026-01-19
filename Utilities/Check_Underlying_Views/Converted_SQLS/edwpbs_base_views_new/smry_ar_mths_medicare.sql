-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_mths_medicare.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_mths_medicare AS SELECT
    smry_ar_mths_medicare.coid,
    smry_ar_mths_medicare.company_code,
    smry_ar_mths_medicare.month_id,
    smry_ar_mths_medicare.ip_balance_amt,
    smry_ar_mths_medicare.op_balance_amt,
    smry_ar_mths_medicare.op_balance_15_pct_amt,
    smry_ar_mths_medicare.discharge_unbill_amt,
    smry_ar_mths_medicare.inhouse_unbill_amt,
    smry_ar_mths_medicare.unbill_balance_20_pct_amt,
    smry_ar_mths_medicare.total_balance_amt,
    smry_ar_mths_medicare.gross_revenue_amt,
    smry_ar_mths_medicare.contractual_amt,
    smry_ar_mths_medicare.net_revenue_amt,
    smry_ar_mths_medicare.ar_days_cnt,
    smry_ar_mths_medicare.dw_last_update_date_time,
    smry_ar_mths_medicare.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_mths_medicare
;
