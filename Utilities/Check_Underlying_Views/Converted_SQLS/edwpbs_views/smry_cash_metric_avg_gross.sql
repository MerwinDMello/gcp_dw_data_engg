-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_cash_metric_avg_gross.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric_avg_gross AS SELECT
    k.service_type_name,
    k.fact_lvl_code,
    k.parent_code,
    k.child_code,
    k.ytd_month_ind,
    k.parent_month_id AS month_id,
    k.payor_short_name,
    CASE
      WHEN k.daysinmth = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE k.gross_revenue_amt / k.daysinmth
    END AS avg_gross_2m_rev_amt
  FROM
    (
      SELECT
          max(a.service_type_name) AS service_type_name,
          a.fact_lvl_code,
          max(a.parent_code) AS parent_code,
          max(a.child_code) AS child_code,
          max(a.ytd_month_ind) AS ytd_month_ind,
          mt.parent_month_id,
          max(a.payor_short_name) AS payor_short_name,
          sum(CASE
            WHEN a.gross_revenue_amt = 0 THEN 0
            ELSE a.days_in_month
          END) AS daysinmth,
          sum(a.gross_revenue_amt) AS gross_revenue_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric AS a
          INNER JOIN (
            SELECT DISTINCT
                b.parent_month_id,
                a_0.month_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_0
                INNER JOIN (
                  SELECT
                      a_1.month_id AS parent_month_id,
                      format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a_1.month_id), '01')), interval -2 MONTH)) AS child_month_id
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_1
                    WHERE a_1.month_id <= CASE
                       format_date('%Y%m', current_date('US/Central'))
                      WHEN '' THEN 0.0
                      ELSE CAST(format_date('%Y%m', current_date('US/Central')) as FLOAT64)
                    END
                ) AS b ON a_0.month_id BETWEEN CASE
                   b.child_month_id
                  WHEN '' THEN 0
                  ELSE CAST(b.child_month_id as INT64)
                END AND b.parent_month_id
                 AND a_0.month_id <> b.parent_month_id
          ) AS mt ON mt.month_id = a.month_id
        WHERE upper(a.ytd_month_ind) = 'MTH'
         AND a.days_in_month IS NOT NULL
         AND upper(a.payor_short_name) = 'UNKNOWN'
        GROUP BY upper(a.service_type_name), 2, upper(a.parent_code), upper(a.child_code), upper(a.ytd_month_ind), 6, upper(a.payor_short_name)
    ) AS k
UNION ALL
SELECT
    k.service_type_name,
    k.fact_lvl_code,
    k.parent_code,
    k.child_code,
    'YTD' AS ytd_month_ind,
    k.parent_month_id AS month_id,
    k.payor_short_name,
    CASE
      WHEN k.daysinmth = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE k.gross_revenue_amt / k.daysinmth
    END AS avg_gross_2m_rev_amt
  FROM
    (
      SELECT
          max(a.service_type_name) AS service_type_name,
          a.fact_lvl_code,
          max(a.parent_code) AS parent_code,
          max(a.child_code) AS child_code,
          max(a.ytd_month_ind) AS ytd_month_ind,
          mt.parent_month_id,
          max(a.payor_short_name) AS payor_short_name,
          sum(CASE
            WHEN a.gross_revenue_amt = 0 THEN 0
            ELSE a.days_in_month
          END) AS daysinmth,
          sum(a.gross_revenue_amt) AS gross_revenue_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric AS a
          INNER JOIN (
            SELECT DISTINCT
                b.parent_month_id,
                a_0.month_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_0
                INNER JOIN (
                  SELECT
                      a_1.month_id AS parent_month_id,
                      format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a_1.month_id), '01')), interval -2 MONTH)) AS child_month_id
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_1
                    WHERE a_1.month_id <= CASE
                       format_date('%Y%m', current_date('US/Central'))
                      WHEN '' THEN 0.0
                      ELSE CAST(format_date('%Y%m', current_date('US/Central')) as FLOAT64)
                    END
                ) AS b ON a_0.month_id BETWEEN CASE
                   b.child_month_id
                  WHEN '' THEN 0
                  ELSE CAST(b.child_month_id as INT64)
                END AND b.parent_month_id
                 AND a_0.month_id <> b.parent_month_id
          ) AS mt ON mt.month_id = a.month_id
        WHERE upper(a.ytd_month_ind) = 'MTH'
         AND a.days_in_month IS NOT NULL
         AND upper(a.payor_short_name) = 'UNKNOWN'
        GROUP BY upper(a.service_type_name), 2, upper(a.parent_code), upper(a.child_code), upper(a.ytd_month_ind), 6, upper(a.payor_short_name)
    ) AS k
;
