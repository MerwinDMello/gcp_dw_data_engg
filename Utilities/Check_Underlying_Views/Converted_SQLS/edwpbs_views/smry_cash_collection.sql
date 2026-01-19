-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_cash_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_collection AS SELECT
    a.rptg_period,
    parse_date('%Y%m%d', concat(a.rptg_period, '01')) AS period_start_date,
    a.month_num,
    a.week_of_month,
    a.ar_transaction_enter_date,
    a.company_code,
    a.coid,
    a.unit_num,
    max(CASE
      WHEN b.payor_name IS NULL THEN 'No_Payor'
      ELSE b.payor_name
    END) AS payor_name,
    max(CASE
      WHEN b.payor_short_name IS NULL THEN 'No_Payor'
      ELSE b.payor_short_name
    END) AS payor_short_name,
    a.up_front_collection_ind,
    sum(a.cash_amt) AS cash_amt,
    sum(a.adjusted_net_revenue_amt) AS adjusted_net_revenue_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_payor AS b ON upper(a.payor_id) = upper(b.payor_id)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS sf ON upper(a.coid) = upper(sf.co_id)
     AND upper(a.company_code) = upper(sf.company_code)
     AND session_user() = sf.user_id
  WHERE upper(a.source_system_code) <> 'D'
   OR a.source_system_code IS NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, upper(CASE
    WHEN b.payor_name IS NULL THEN 'No_Payor'
    ELSE b.payor_name
  END), upper(CASE
    WHEN b.payor_short_name IS NULL THEN 'No_Payor'
    ELSE b.payor_short_name
  END), 11
;
