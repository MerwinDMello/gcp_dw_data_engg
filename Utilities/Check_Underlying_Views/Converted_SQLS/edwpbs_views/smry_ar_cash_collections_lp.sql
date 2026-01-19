-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_cash_collections_lp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_cash_collections_lp AS SELECT
    a.rptg_period,
    parse_date('%Y%m%d', concat(a.rptg_period, '01')) AS period_start_date,
    a.month_num,
    a.week_of_month,
    a.ar_transaction_enter_date,
    a.company_code,
    a.coid,
    a.unit_num,
    a.derived_patient_type_code,
    ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.parent_payor_name,
    a.payor_id,
    a.iplan_id,
    a.up_front_collection_ind,
    a.source_system_code,
    ROUND(a.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    ROUND(a.adjusted_net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS adjusted_net_revenue_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections_lp AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(a.coid) = upper(sf.co_id)
     AND upper(a.company_code) = upper(sf.company_code)
     AND session_user() = sf.user_id
UNION ALL
SELECT
    max(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval 2 MONTH))) AS rptg_period,
    parse_date('%Y%m%d', concat(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval 2 MONTH)), '01')) AS period_start_date,
    0 AS month_num,
    0 AS week_of_month,
    DATE '1901-01-01' AS ar_transaction_enter_date,
    max(fd.company_code) AS company_code,
    max(fd.coid) AS coid,
    max(fd.unit_num) AS unit_num,
    'NA' AS derived_patient_type_code,
    CAST(999 as NUMERIC) AS financial_class_code,
    CAST(999 as NUMERIC) AS payor_dw_id,
    '0' AS parent_payor_name,
    '999' AS payor_id,
    999 AS iplan_id,
    'N' AS up_front_collection_ind,
    'A' AS source_system_code,
    CAST(0 as NUMERIC) AS cash_amt,
    CAST(ROUND(sum(a.metric_value), 3, 'ROUND_HALF_EVEN') as NUMERIC) AS adjusted_net_revenue_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.sla_metric_adjustment AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_dimension AS fd ON upper(fd.coid) = upper(a.parent_code)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(a.parent_code) = upper(sf.co_id)
     AND upper(fd.company_code) = upper(sf.company_code)
     AND sf.user_id = session_user()
  WHERE upper(a.metric_sub_code) > '300'
   AND upper(a.metric_sub_code) < '400'
   AND upper(a.service_type_name) = 'HOSPITAL'
   AND a.fact_lvl_code = 7.0
   AND upper(a.metric_code) = '2'
   AND upper(a.corporate_code) = 'H-LIF'
  GROUP BY upper(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval 2 MONTH))), 2, 3, 3, 5, upper(fd.company_code), upper(fd.coid), upper(fd.unit_num), 9, 10, 11, 14, 17
;
