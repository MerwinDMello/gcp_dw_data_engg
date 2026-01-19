-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_metric_3m_anr.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_metric_3m_anr AS SELECT
    k.service_type_name,
    k.fact_lvl_code,
    k.parent_code,
    k.child_code,
    k.ytd_month_ind,
    k.parent_month_id AS month_id,
    k.payor_type,
    CASE
      WHEN k.daysinmth = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE k.adj_net_rev_amt / k.daysinmth
    END AS avg_dly_anr_l3m
  FROM
    (
      SELECT
          max(a.service_type_name) AS service_type_name,
          a.fact_lvl_code,
          max(a.parent_code) AS parent_code,
          max(a.child_code) AS child_code,
          a.ytd_month_ind,
          mt.parent_month_id,
          a.payor_type,
          sum(CASE
            WHEN a.adj_net_rev_amt = 0 THEN CAST(0 as BIGNUMERIC)
            ELSE a.days_in_month
          END) AS daysinmth,
          sum(a.adj_net_rev_amt) AS adj_net_rev_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_metric_other AS a
          INNER JOIN (
            SELECT DISTINCT
                b.parent_month_id,
                a_0.month_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_0
                INNER JOIN (
                  SELECT
                      a_1.month_id AS parent_month_id,
                      substr(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a_1.month_id), '01')), interval -2 MONTH)), 1, 6) AS child_month_id
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_1
                    WHERE a_1.month_id <= CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', current_date('US/Central')), 1, 6)) as FLOAT64)
                ) AS b ON a_0.month_id BETWEEN CAST(bqutil.fn.cw_td_normalize_number(b.child_month_id) as INT64) AND b.parent_month_id
          ) AS mt ON mt.month_id = a.month_id
        WHERE upper(a.ytd_month_ind) = 'MTH'
         AND upper(a.payor_type) = 'N'
         AND a.days_in_month IS NOT NULL
        GROUP BY upper(a.service_type_name), 2, upper(a.parent_code), upper(a.child_code), 5, 6, 7
    ) AS k
UNION ALL
SELECT
    k.service_type_name,
    k.fact_lvl_code,
    k.parent_code,
    k.child_code,
    'YTD' AS ytd_month_ind,
    k.parent_month_id AS month_id,
    k.payor_type,
    CASE
      WHEN k.daysinmth = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE k.adj_net_rev_amt / k.daysinmth
    END AS avg_dly_anr_l3m
  FROM
    (
      SELECT
          max(a.service_type_name) AS service_type_name,
          a.fact_lvl_code,
          max(a.parent_code) AS parent_code,
          max(a.child_code) AS child_code,
          a.ytd_month_ind,
          mt.parent_month_id,
          a.payor_type,
          sum(CASE
            WHEN a.adj_net_rev_amt = 0 THEN CAST(0 as BIGNUMERIC)
            ELSE a.days_in_month
          END) AS daysinmth,
          sum(a.adj_net_rev_amt) AS adj_net_rev_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_metric_other AS a
          INNER JOIN (
            SELECT DISTINCT
                b.parent_month_id,
                a_0.month_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_0
                INNER JOIN (
                  SELECT
                      a_1.month_id AS parent_month_id,
                      substr(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a_1.month_id), '01')), interval -2 MONTH)), 1, 6) AS child_month_id
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS a_1
                    WHERE a_1.month_id <= CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', current_date('US/Central')), 1, 6)) as FLOAT64)
                ) AS b ON a_0.month_id BETWEEN CAST(bqutil.fn.cw_td_normalize_number(b.child_month_id) as INT64) AND b.parent_month_id
          ) AS mt ON mt.month_id = a.month_id
        WHERE upper(a.ytd_month_ind) = 'MTH'
         AND upper(a.payor_type) = 'N'
         AND a.days_in_month IS NOT NULL
        GROUP BY upper(a.service_type_name), 2, upper(a.parent_code), upper(a.child_code), 5, 6, 7
    ) AS k
;
