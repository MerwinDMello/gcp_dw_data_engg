-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_analysis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_analysis AS SELECT
    net_revenue_impact_analysis.patient_dw_id,
    net_revenue_impact_analysis.coid,
    net_revenue_impact_analysis.payor_dw_id,
    net_revenue_impact_analysis.metric_code,
    net_revenue_impact_analysis.month_id,
    net_revenue_impact_analysis.pe_date,
    net_revenue_impact_analysis.week_month_code,
    net_revenue_impact_analysis.financial_class_code,
    net_revenue_impact_analysis.company_code,
    net_revenue_impact_analysis.unit_num,
    net_revenue_impact_analysis.pat_acct_num,
    net_revenue_impact_analysis.iplan_id,
    net_revenue_impact_analysis.discharge_date,
    net_revenue_impact_analysis.patient_type_code,
    net_revenue_impact_analysis.metric_amt,
    net_revenue_impact_analysis.dw_last_update_date_time,
    net_revenue_impact_analysis.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.net_revenue_impact_analysis
;
