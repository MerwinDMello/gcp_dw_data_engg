-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_underpayment_pmt_act.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_underpayment_pmt_act AS SELECT
    net_revenue_impact_underpayment_pmt_act.reporting_date,
    net_revenue_impact_underpayment_pmt_act.patient_dw_id,
    net_revenue_impact_underpayment_pmt_act.payor_dw_id,
    net_revenue_impact_underpayment_pmt_act.company_code,
    net_revenue_impact_underpayment_pmt_act.coid,
    net_revenue_impact_underpayment_pmt_act.admission_date,
    net_revenue_impact_underpayment_pmt_act.unit_num,
    net_revenue_impact_underpayment_pmt_act.pat_acct_num,
    net_revenue_impact_underpayment_pmt_act.iplan_id,
    net_revenue_impact_underpayment_pmt_act.financial_class_code,
    net_revenue_impact_underpayment_pmt_act.patient_type_code,
    net_revenue_impact_underpayment_pmt_act.discrepancy_origination_date,
    net_revenue_impact_underpayment_pmt_act.discharge_date,
    net_revenue_impact_underpayment_pmt_act.under_payment_activity_amt,
    net_revenue_impact_underpayment_pmt_act.payment_discrepancy_ind,
    net_revenue_impact_underpayment_pmt_act.take_back_ind,
    net_revenue_impact_underpayment_pmt_act.dw_last_update_date_time,
    net_revenue_impact_underpayment_pmt_act.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.net_revenue_impact_underpayment_pmt_act
;
