-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_analysis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_analysis
   OPTIONS(description='This table can be used to determine Monthly/Weekly data such as Credit Refund, Denial, Discrepancy Inventory, Underpayment accounts etc that impacts net revenue.')
  AS SELECT
      ROUND(net_revenue_impact_analysis.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_analysis.coid,
      ROUND(net_revenue_impact_analysis.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_analysis.metric_code,
      net_revenue_impact_analysis.month_id,
      net_revenue_impact_analysis.pe_date,
      net_revenue_impact_analysis.week_month_code,
      ROUND(net_revenue_impact_analysis.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_analysis.company_code,
      net_revenue_impact_analysis.unit_num,
      ROUND(net_revenue_impact_analysis.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_analysis.iplan_id,
      net_revenue_impact_analysis.discharge_date,
      net_revenue_impact_analysis.patient_type_code,
      ROUND(net_revenue_impact_analysis.metric_amt, 3, 'ROUND_HALF_EVEN') AS metric_amt,
      net_revenue_impact_analysis.dw_last_update_date_time,
      net_revenue_impact_analysis.source_system_code
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.net_revenue_impact_analysis
  ;
