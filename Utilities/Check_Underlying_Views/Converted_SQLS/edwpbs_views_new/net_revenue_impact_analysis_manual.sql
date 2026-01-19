-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_analysis_manual.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.net_revenue_impact_analysis_manual AS SELECT
    a.patient_dw_id,
    a.coid,
    CASE
      WHEN substr(trim(format('%#20.0f', a.payor_dw_id)), 1, 1) = '9' THEN 'N'
      ELSE 'Y'
    END AS covid_test_ind,
    CASE
      WHEN substr(trim(format('%#20.0f', a.payor_dw_id)), 2, 1) = '9' THEN 'N'
      ELSE 'Y'
    END AS recovery_ind,
    CASE
      WHEN substr(trim(format('%#20.0f', a.payor_dw_id)), 3, 1) = '9' THEN 'N'
      ELSE 'Y'
    END AS res_discr_ind,
    CASE
      WHEN substr(trim(format('%#20.0f', a.payor_dw_id)), 4, 1) = '9' THEN 'N'
      ELSE 'Y'
    END AS rev_recog_reason_ind,
    a.metric_code,
    a.month_id,
    a.pe_date,
    a.week_month_code,
    a.financial_class_code,
    a.company_code,
    a.unit_num,
    a.pat_acct_num,
    a.iplan_id,
    a.discharge_date,
    CASE
       CAST(bqutil.fn.cw_td_normalize_number(a.patient_type_code) as FLOAT64)
      WHEN 100 THEN 'Unprocessed Interim Bill_{O}_[15]'
      WHEN 200 THEN 'Unprocessed Late Charges_{O}_[15]'
      WHEN 300 THEN 'Unprocessed Partial Chgs_{O}_[15]'
      WHEN 400 THEN 'Unprocessed Split Bill_{O}_[15]'
      WHEN 999 THEN CAST(NULL as STRING)
    END AS reason_code,
    a.metric_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_analysis AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
  WHERE a.source_system_code = 'M'
;
