-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_day_past.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_day_past AS SELECT
    a.date_id,
    concat(a.month_num_desc_s, '-', format('%02d', a.month_num)) AS month_desc_s,
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
          format('', max(smry_ar_cash_collections.ar_transaction_enter_date)) AS max_tran_month,
          CASE
            WHEN upper(substr(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)), 5, 2)) = '01' THEN CAST(CASE
               format('', max(smry_ar_cash_collections.ar_transaction_enter_date))
              WHEN '' THEN 0.0
              ELSE CAST(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)) as FLOAT64)
            END - 89 as INT64)
            ELSE CAST(CASE
               format('', max(smry_ar_cash_collections.ar_transaction_enter_date))
              WHEN '' THEN 0.0
              ELSE CAST(format('', max(smry_ar_cash_collections.ar_transaction_enter_date)) as FLOAT64)
            END - 1 as INT64)
          END AS prev_tran_month
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections
    ) AS b ON (extract(YEAR from a.date_id) - 1900) * 10000 + extract(MONTH from a.date_id) * 100 + extract(DAY from a.date_id) <= b.max_tran_date
     AND a.month_id >= b.prev_tran_month
;
