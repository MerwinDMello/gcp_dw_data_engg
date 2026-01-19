-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.smry_ar_metric AS SELECT
    smry_ar_metric.service_type_name,
    smry_ar_metric.fact_lvl_code,
    smry_ar_metric.parent_code,
    smry_ar_metric.child_code,
    smry_ar_metric.ytd_month_ind,
    smry_ar_metric.month_id,
    smry_ar_metric.patient_type_desc,
    ROUND(smry_ar_metric.bad_debt_wo_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_wo_amt,
    ROUND(smry_ar_metric.total_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_balance_amt,
    ROUND(smry_ar_metric.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(smry_ar_metric.medicare_ar_day_cnt, 3, 'ROUND_HALF_EVEN') AS medicare_ar_day_cnt,
    ROUND(smry_ar_metric.mc_fc9_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mc_fc9_total_charge_amt,
    ROUND(smry_ar_metric.mcaid_fc3_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_fc3_total_charge_amt,
    ROUND(smry_ar_metric.mcaid_conv_ttl_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_conv_ttl_charge_amt,
    ROUND(smry_ar_metric.self_pay_3_mth_revenue_avg, 3, 'ROUND_HALF_EVEN') AS self_pay_3_mth_revenue_avg,
    CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END AS prior_month_id,
    CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 2
       OR CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 90
      ELSE smry_ar_metric.month_id - 2
    END AS prior_2_month_id,
    parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')) AS curr_mth_start,
    DATE(parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')) + INTERVAL 1 MONTH) AS next_mth_start,
    date_diff(DATE(parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')) + INTERVAL 1 MONTH), parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')), DAY) AS curr_month_days,
    date_diff(parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')), parse_date('%Y-%m-%d', concat(substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 1, 4), '-', substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 5, 2), '-01')), DAY) AS prev_month_days,
    date_diff(parse_date('%Y-%m-%d', concat(substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 1, 4), '-', substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 5, 2), '-01')), parse_date('%Y-%m-%d', concat(substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 2
       OR CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 90
      ELSE smry_ar_metric.month_id - 2
    END), 1, 4), '-', substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 2
       OR CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 90
      ELSE smry_ar_metric.month_id - 2
    END), 5, 2), '-01')), DAY) AS prev_2_month_days,
    date_diff(DATE(parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')) + INTERVAL 1 MONTH), parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')), DAY) + date_diff(parse_date('%Y-%m-%d', concat(substr(format('%11d', smry_ar_metric.month_id), 1, 4), '-', substr(format('%11d', smry_ar_metric.month_id), 5, 2), '-01')), parse_date('%Y-%m-%d', concat(substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 1, 4), '-', substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 5, 2), '-01')), DAY) + date_diff(parse_date('%Y-%m-%d', concat(substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 1, 4), '-', substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 89
      ELSE smry_ar_metric.month_id - 1
    END), 5, 2), '-01')), parse_date('%Y-%m-%d', concat(substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 2
       OR CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 90
      ELSE smry_ar_metric.month_id - 2
    END), 1, 4), '-', substr(format('%11d', CASE
      WHEN CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 2
       OR CASE
         substr(format('%11d', smry_ar_metric.month_id), 5, 2)
        WHEN '' THEN 0.0
        ELSE CAST(substr(format('%11d', smry_ar_metric.month_id), 5, 2) as FLOAT64)
      END = 1 THEN smry_ar_metric.month_id - 90
      ELSE smry_ar_metric.month_id - 2
    END), 5, 2), '-01')), DAY) AS total_3_month_days,
    smry_ar_metric.dw_last_update_date_time,
    smry_ar_metric.source_system_code
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.smry_ar_metric
;
