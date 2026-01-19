-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_analysis_manual.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.net_revenue_impact_analysis_manual AS SELECT
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
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
    ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    a.company_code,
    a.unit_num,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.iplan_id,
    a.discharge_date,
    CASE
       CASE
         a.patient_type_code
        WHEN '' THEN 0.0
        ELSE CAST(a.patient_type_code as FLOAT64)
      END
      WHEN 100 THEN 'Unprocessed Interim Bill_{O}_[15]'
      WHEN 200 THEN 'Unprocessed Late Charges_{O}_[15]'
      WHEN 300 THEN 'Unprocessed Partial Chgs_{O}_[15]'
      WHEN 400 THEN 'Unprocessed Split Bill_{O}_[15]'
      WHEN 999 THEN CAST(NULL as STRING)
    END AS reason_code,
    ROUND(a.metric_amt, 3, 'ROUND_HALF_EVEN') AS metric_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_analysis AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
  WHERE upper(a.source_system_code) = 'M'
;
