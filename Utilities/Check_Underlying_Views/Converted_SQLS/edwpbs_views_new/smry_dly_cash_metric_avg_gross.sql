-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_dly_cash_metric_avg_gross.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_dly_cash_metric_avg_gross AS SELECT
    dt.date_id,
    c.service_type_name,
    c.fact_lvl_code,
    c.parent_code,
    c.child_code,
    'DLY' AS ytd_month_ind,
    c.payor_short_name,
    c.avg_gross_2m_rev_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric_avg_gross AS c
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_date AS dt ON dt.month_id = c.month_id
  WHERE upper(c.ytd_month_ind) = 'MTH'
   AND dt.date_id BETWEEN date_sub(current_date('US/Central'), interval 90 DAY) AND date_sub(current_date('US/Central'), interval 2 DAY)
UNION ALL
SELECT
    dt.date_id,
    c.service_type_name,
    c.fact_lvl_code,
    c.parent_code,
    c.child_code,
    'DLY' AS ytd_month_ind,
    c.payor_short_name,
    c.avg_gross_2m_rev_amt
  FROM
    (
      SELECT
          smry_cash_metric_avg_gross.service_type_name,
          smry_cash_metric_avg_gross.fact_lvl_code,
          smry_cash_metric_avg_gross.parent_code,
          smry_cash_metric_avg_gross.child_code,
          smry_cash_metric_avg_gross.payor_short_name,
          smry_cash_metric_avg_gross.avg_gross_2m_rev_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric_avg_gross
        WHERE smry_cash_metric_avg_gross.month_id = CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', date_add(current_date('US/Central'), interval -2 MONTH)), 1, 6)) as INT64)
         AND extract(DAY from current_date('US/Central')) BETWEEN 1 AND 11
         AND upper(smry_cash_metric_avg_gross.ytd_month_ind) = 'MTH'
      UNION ALL
      SELECT
          smry_cash_metric_avg_gross.service_type_name,
          smry_cash_metric_avg_gross.fact_lvl_code,
          smry_cash_metric_avg_gross.parent_code,
          smry_cash_metric_avg_gross.child_code,
          smry_cash_metric_avg_gross.payor_short_name,
          smry_cash_metric_avg_gross.avg_gross_2m_rev_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric_avg_gross
        WHERE smry_cash_metric_avg_gross.month_id = CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)), 1, 6)) as INT64)
         AND extract(DAY from current_date('US/Central')) > 11
         AND upper(smry_cash_metric_avg_gross.ytd_month_ind) = 'MTH'
    ) AS c
    CROSS JOIN (
      SELECT
          lu_date.date_id,
          lu_date.month_id,
          lu_date.pe_date
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.lu_date
        WHERE lu_date.date_id BETWEEN parse_date('%Y%m%d', concat(substr(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)), 1, 6), '01')) AND date_sub(current_date('US/Central'), interval 2 DAY)
         AND extract(DAY from current_date('US/Central')) BETWEEN 1 AND 11
      UNION ALL
      SELECT
          lu_date.date_id,
          lu_date.month_id,
          lu_date.pe_date
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.lu_date
        WHERE lu_date.date_id BETWEEN parse_date('%Y%m%d', concat(substr(format_date('%Y%m', current_date('US/Central')), 1, 6), '01')) AND date_sub(current_date('US/Central'), interval 2 DAY)
         AND extract(DAY from current_date('US/Central')) > 11
    ) AS dt
;
