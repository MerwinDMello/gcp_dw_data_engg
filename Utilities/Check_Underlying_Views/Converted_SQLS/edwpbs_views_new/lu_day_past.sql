-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_day_past.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_day_past AS SELECT
    a.date_id,
    concat(a.month_num_desc_s, '-', substr(format('%02d', a.month_num), 1, 2)) AS month_desc_s,
    DATE(a.date_id - INTERVAL 1 DAY) AS prior_date_id,
    DATE(a.date_id - INTERVAL 2 DAY) AS prior_2_date_id,
    DATE(a.date_id - INTERVAL 3 DAY) AS prior_3_date_id,
    DATE(a.date_id - INTERVAL 7 DAY) AS prior_week_date_id,
    date_add(a.date_id, interval -2 MONTH) AS prior_2_month_date_id,
    b.max_tran_date,
    CASE
      WHEN (extract(YEAR from a.date_id) - 1900) * 10000 + extract(MONTH from a.date_id) * 100 + extract(DAY from a.date_id) = b.max_tran_date THEN 'C'
      ELSE 'N'
    END AS tran_date_ind_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_date AS a
    INNER JOIN (
      SELECT
          max(smry_ar_cash_collections.ar_transaction_enter_date) AS max_tran_date,
          substr(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)), 1, 6) AS max_tran_month,
          CASE
            WHEN upper(substr(substr(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)), 1, 6), 5, 2)) = '01' THEN CAST(CAST(bqutil.fn.cw_td_normalize_number(substr(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)), 1, 6)) as FLOAT64) - 89 as INT64)
            ELSE CAST(CAST(bqutil.fn.cw_td_normalize_number(substr(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)), 1, 6)) as FLOAT64) - 1 as INT64)
          END AS prev_tran_month
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections
    ) AS b ON (extract(YEAR from a.date_id) - 1900) * 10000 + extract(MONTH from a.date_id) * 100 + extract(DAY from a.date_id) <= b.max_tran_date
     AND a.month_id >= b.prev_tran_month
;
