-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_armap_ufc_goal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_armap_ufc_goal AS SELECT
    curr.month_id,
    curr.coid,
    curr.unit_num,
    ROUND(curr.upfront_collection_pcnt, 5, 'ROUND_HALF_EVEN') AS upfront_collection_pcnt,
    curr.net_revenue,
    prev.prev_net_rev AS avg_net_rev_two_prev_months,
    ROUND(curr.upfront_collection_pcnt * prev.prev_net_rev, 5, 'ROUND_HALF_EVEN') AS ufc_goal
  FROM
    (
      SELECT
          b.month_id,
          max(a.coid) AS coid,
          max(a.unit_num) AS unit_num,
          a.upfront_collection_pcnt,
          sum(b.net_revenue) AS net_revenue
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_facility_goal AS a
          INNER JOIN (
            SELECT
                max(c.parent_code) AS parent_code,
                c.month_id,
                sum(c.net_revenue) AS net_revenue
              FROM
                (
                  SELECT
                      max(smry_ar_metric_other.parent_code) AS parent_code,
                      smry_ar_metric_other.month_id,
                      sum(smry_ar_metric_other.bad_debt_amt) AS net_revenue
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_metric_other
                    WHERE upper(smry_ar_metric_other.service_type_name) = 'HOSPITAL'
                     AND upper(smry_ar_metric_other.ytd_month_ind) = 'MTH'
                     AND smry_ar_metric_other.fact_lvl_code = 7
                    GROUP BY upper(smry_ar_metric_other.parent_code), 2
                  UNION DISTINCT
                  SELECT
                      max(smry_cash_metric.parent_code) AS parent_code,
                      smry_cash_metric.month_id,
                      sum(smry_cash_metric.net_revenue_amt) AS net_revenue
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_cash_metric
                    WHERE upper(smry_cash_metric.service_type_name) = 'HOSPITAL'
                     AND upper(smry_cash_metric.ytd_month_ind) = 'MTH'
                     AND smry_cash_metric.fact_lvl_code = 7
                    GROUP BY upper(smry_cash_metric.parent_code), 2
                ) AS c
              GROUP BY upper(c.parent_code), 2
          ) AS b ON upper(a.coid) = upper(b.parent_code)
           AND b.month_id > 201212.0
        GROUP BY 1, upper(a.coid), upper(a.unit_num), 4
    ) AS curr
    INNER JOIN (
      SELECT
          k.curr_mth AS month_id,
          max(b.parent_code) AS coid,
          sum(b.net_revenue) / 2 AS prev_net_rev
        FROM
          (
            SELECT
                max(a.parent_code) AS parent_code,
                a.month_id,
                sum(a.net_revenue) AS net_revenue
              FROM
                (
                  SELECT
                      max(smry_ar_metric_other.parent_code) AS parent_code,
                      smry_ar_metric_other.month_id,
                      sum(smry_ar_metric_other.bad_debt_amt) AS net_revenue
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_metric_other
                    WHERE upper(smry_ar_metric_other.service_type_name) = 'HOSPITAL'
                     AND upper(smry_ar_metric_other.ytd_month_ind) = 'MTH'
                     AND smry_ar_metric_other.fact_lvl_code = 7
                     AND upper(smry_ar_metric_other.parent_code) = '16150'
                    GROUP BY upper(smry_ar_metric_other.parent_code), 2
                  UNION DISTINCT
                  SELECT
                      max(smry_cash_metric.parent_code) AS parent_code,
                      smry_cash_metric.month_id,
                      sum(smry_cash_metric.net_revenue_amt) AS net_revenue
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_cash_metric
                    WHERE upper(smry_cash_metric.service_type_name) = 'HOSPITAL'
                     AND upper(smry_cash_metric.ytd_month_ind) = 'MTH'
                     AND smry_cash_metric.fact_lvl_code = 7
                    GROUP BY upper(smry_cash_metric.parent_code), 2
                ) AS a
              GROUP BY upper(a.parent_code), 2
          ) AS b
          INNER JOIN (
            SELECT
                pt.month_id AS curr_mth,
                m.month_id AS prev_mths
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_views.lu_year_month_past AS pt
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.lu_month AS m ON m.month_id BETWEEN pt.prior_2_month_id AND pt.prior_month_id
          ) AS k ON k.prev_mths = b.month_id
        GROUP BY 1, upper(b.parent_code)
    ) AS prev ON upper(curr.coid) = upper(prev.coid)
     AND curr.month_id = prev.month_id
;
