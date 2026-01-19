-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_denial AS SELECT
    net_revenue_impact_denial.reporting_date,
    net_revenue_impact_denial.patient_dw_id,
    net_revenue_impact_denial.iplan_id,
    net_revenue_impact_denial.iplan_insurance_order_num,
    net_revenue_impact_denial.payor_dw_id,
    net_revenue_impact_denial.company_code,
    net_revenue_impact_denial.coid,
    net_revenue_impact_denial.unit_num,
    net_revenue_impact_denial.pat_acct_num,
    net_revenue_impact_denial.financial_class_code,
    net_revenue_impact_denial.denial_status_code,
    net_revenue_impact_denial.patient_type_code,
    net_revenue_impact_denial.write_off_denial_amt,
    net_revenue_impact_denial.overturned_denial_amt,
    net_revenue_impact_denial.dw_last_update_date_time,
    net_revenue_impact_denial.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.net_revenue_impact_denial
;
