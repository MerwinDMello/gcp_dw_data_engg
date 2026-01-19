-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS sf ON upper(a.coid) = upper(sf.co_id)
     AND upper(a.company_code) = upper(sf.company_code)
     AND session_user() = sf.user_id
  WHERE upper(a.source_system_code) <> 'D'
   OR a.source_system_code IS NULL
  GROUP BY 1, 2, 3, 4, 5
;
