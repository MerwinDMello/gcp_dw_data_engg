-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    a.financial_class_code,
    a.payor_dw_id,
    a.parent_payor_name,
    a.payor_id,
    a.iplan_id,
    a.up_front_collection_ind,
    a.source_system_code,
    a.cash_amt,
    a.adjusted_net_revenue_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections_lp AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON a.coid = sf.co_id
     AND a.company_code = sf.company_code
     AND session_user() = sf.user_id
UNION ALL
SELECT
    cw_ss.monthid AS rptg_period,
    cw_ss.period_start_date,
    cw_ss.month_num AS month_num,
    cw_ss.week_of_month AS week_of_month,
    cw_ss.ar_transaction_enter_date AS ar_transaction_enter_date,
    cw_ss.company_code AS company_code,
    cw_ss.coid AS coid,
    cw_ss.unit_num AS unit_num,
    cw_ss.derrived_patient_type_code AS derived_patient_type_code,
    cw_ss.financial_class_code AS financial_class_code,
    cw_ss.payor_dw_id AS payor_dw_id,
    cw_ss.parent_payor_name AS parent_payor_name,
    cw_ss.payor_id AS payor_id,
    cw_ss.iplan_id AS iplan_id,
    cw_ss.up_front_collection_ind AS up_front_collection_ind,
    cw_ss.source_system_code AS source_system_code,
    cw_ss.cash_amt AS cash_amt,
    cw_ss.metric_value AS adjusted_net_revenue_amt
  FROM
    (
      SELECT
          substr(max(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval 2 MONTH))), 1, 6) AS monthid,
          parse_date('%Y%m%d', concat(substr(max(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval 2 MONTH))), 1, 6), '01')) AS period_start_date,
          0 AS month_num,
          0 AS week_of_month,
          DATE '1901-01-01' AS ar_transaction_enter_date,
          fd.company_code,
          fd.coid,
          fd.unit_num,
          'NA' AS derrived_patient_type_code,
          999 AS financial_class_code,
          999 AS payor_dw_id,
          '0' AS parent_payor_name,
          '999' AS payor_id,
          999 AS iplan_id,
          'N' AS up_front_collection_ind,
          'A' AS source_system_code,
          0 AS cash_amt,
          sum(a.metric_value) AS metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.sla_metric_adjustment AS a
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_dimension AS fd ON fd.coid = a.parent_code
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON a.parent_code = sf.co_id
           AND fd.company_code = sf.company_code
           AND sf.user_id = session_user()
        WHERE upper(a.metric_sub_code) > '300'
         AND upper(a.metric_sub_code) < '400'
         AND upper(a.service_type_name) = 'HOSPITAL'
         AND a.fact_lvl_code = 7.0
         AND upper(a.metric_code) = '2'
         AND upper(a.corporate_code) = 'H-LIF'
        GROUP BY upper(substr(format_date('%Y%m', date_add(parse_date('%Y%m%d', concat(format('%11d', a.month_id), '01')), interval 2 MONTH)), 1, 6)), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
    ) AS cw_ss
;
