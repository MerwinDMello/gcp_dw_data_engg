-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_metric AS SELECT
    smry_ar_metric.service_type_name,
    smry_ar_metric.fact_lvl_code,
    smry_ar_metric.parent_code,
    smry_ar_metric.child_code,
    smry_ar_metric.ytd_month_ind,
    smry_ar_metric.month_id,
    smry_ar_metric.patient_type_desc,
    smry_ar_metric.bad_debt_wo_amt,
    smry_ar_metric.total_balance_amt,
    smry_ar_metric.net_revenue_amt,
    smry_ar_metric.medicare_ar_day_cnt,
    smry_ar_metric.mc_fc9_total_charge_amt,
    smry_ar_metric.mcaid_fc3_total_charge_amt,
    smry_ar_metric.mcaid_conv_ttl_charge_amt,
    smry_ar_metric.self_pay_3_mth_revenue_avg,
    prior_month_id AS prior_month_id,
    prior_2_month_id AS prior_2_month_id,
    curr_mth_start AS curr_mth_start,
    next_mth_start AS next_mth_start,
    curr_month_days AS curr_month_days,
    prev_month_days AS prev_month_days,
    prev_2_month_days AS prev_2_month_days,
    curr_month_days + prev_month_days + prev_2_month_days AS total_3_month_days,
    smry_ar_metric.dw_last_update_date_time,
    smry_ar_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_metric
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN CAST(bqutil.fn.cw_td_normalize_number(substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 5, 2)) as FLOAT64) = 2
         OR CAST(bqutil.fn.cw_td_normalize_number(substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 5, 2)) as FLOAT64) = 1 THEN smry_ar_metric.month_id - 90
        ELSE smry_ar_metric.month_id - 2
      END
    ]) AS prior_2_month_id
    CROSS JOIN UNNEST(ARRAY[
      CASE
        WHEN CAST(bqutil.fn.cw_td_normalize_number(substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 5, 2)) as FLOAT64) = 1 THEN smry_ar_metric.month_id - 89
        ELSE smry_ar_metric.month_id - 1
      END
    ]) AS prior_month_id
    CROSS JOIN UNNEST(ARRAY[
      date_diff(parse_date('%Y-%m-%d', concat(substr(CAST(prior_month_id as STRING), 1, 4), '-', substr(CAST(prior_month_id as STRING), 5, 2), '-01')), parse_date('%Y-%m-%d', concat(substr(CAST(prior_2_month_id as STRING), 1, 4), '-', substr(CAST(prior_2_month_id as STRING), 5, 2), '-01')), DAY)
    ]) AS prev_2_month_days
    CROSS JOIN UNNEST(ARRAY[
      date_diff(parse_date('%Y-%m-%d', concat(substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 1, 4), '-', substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 5, 2), '-01')), parse_date('%Y-%m-%d', concat(substr(CAST(prior_month_id as STRING), 1, 4), '-', substr(CAST(prior_month_id as STRING), 5, 2), '-01')), DAY)
    ]) AS prev_month_days
    CROSS JOIN UNNEST(ARRAY[
      parse_date('%Y-%m-%d', concat(substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 1, 4), '-', substr(CAST(/* expression of unknown or erroneous type */ smry_ar_metric.month_id as STRING), 5, 2), '-01'))
    ]) AS curr_mth_start
    CROSS JOIN UNNEST(ARRAY[
      DATE(curr_mth_start + INTERVAL 1 MONTH)
    ]) AS next_mth_start
    CROSS JOIN UNNEST(ARRAY[
      date_diff(next_mth_start, curr_mth_start, DAY)
    ]) AS curr_month_days
;
