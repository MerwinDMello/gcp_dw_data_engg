-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_cash_adj_net_rev.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_cash_adj_net_rev AS SELECT
    a.rptg_period,
    parse_date('%Y%m%d', concat(a.rptg_period, '01')) AS period_start_date,
    a.company_code,
    a.coid,
    a.unit_num,
    sum(a.adjusted_net_revenue_amt) AS adjusted_net_rev_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS sf ON a.coid = sf.co_id
     AND a.company_code = sf.company_code
     AND session_user() = sf.user_id
  WHERE upper(a.source_system_code) <> 'D'
   OR a.source_system_code IS NULL
  GROUP BY 1, 2, 3, 4, 5
;
