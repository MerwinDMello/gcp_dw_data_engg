-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_credit_refund AS SELECT
    net_revenue_impact_credit_refund.reporting_date,
    net_revenue_impact_credit_refund.patient_dw_id,
    net_revenue_impact_credit_refund.iplan_id,
    net_revenue_impact_credit_refund.company_code,
    net_revenue_impact_credit_refund.coid,
    net_revenue_impact_credit_refund.unit_num,
    net_revenue_impact_credit_refund.payor_dw_id,
    net_revenue_impact_credit_refund.pat_acct_num,
    net_revenue_impact_credit_refund.admission_date,
    net_revenue_impact_credit_refund.discharge_date,
    net_revenue_impact_credit_refund.entered_date,
    net_revenue_impact_credit_refund.patient_type_code,
    net_revenue_impact_credit_refund.financial_class_code,
    net_revenue_impact_credit_refund.payment_discrepancy_ind,
    net_revenue_impact_credit_refund.cm_dcrp_rslvd_ind,
    net_revenue_impact_credit_refund.refund_amt,
    net_revenue_impact_credit_refund.dw_last_update_date_time,
    net_revenue_impact_credit_refund.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.net_revenue_impact_credit_refund
;
