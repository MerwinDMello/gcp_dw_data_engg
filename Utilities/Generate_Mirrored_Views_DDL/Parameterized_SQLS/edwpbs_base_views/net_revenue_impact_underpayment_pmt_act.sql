-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_underpayment_pmt_act.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.net_revenue_impact_underpayment_pmt_act
   OPTIONS(description='Daily snapshot of Underpayment Recoveries to analyze the impact with Discrepancies raised and finally the impat on Net Revenue.\r\rThis table tracks the payment activity related to underpayment.')
  AS SELECT
      net_revenue_impact_underpayment_pmt_act.reporting_date,
      ROUND(net_revenue_impact_underpayment_pmt_act.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(net_revenue_impact_underpayment_pmt_act.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_underpayment_pmt_act.company_code,
      net_revenue_impact_underpayment_pmt_act.coid,
      net_revenue_impact_underpayment_pmt_act.admission_date,
      net_revenue_impact_underpayment_pmt_act.unit_num,
      ROUND(net_revenue_impact_underpayment_pmt_act.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_underpayment_pmt_act.iplan_id,
      ROUND(net_revenue_impact_underpayment_pmt_act.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_underpayment_pmt_act.patient_type_code,
      net_revenue_impact_underpayment_pmt_act.discrepancy_origination_date,
      net_revenue_impact_underpayment_pmt_act.discharge_date,
      ROUND(net_revenue_impact_underpayment_pmt_act.under_payment_activity_amt, 4, 'ROUND_HALF_EVEN') AS under_payment_activity_amt,
      net_revenue_impact_underpayment_pmt_act.payment_discrepancy_ind,
      net_revenue_impact_underpayment_pmt_act.take_back_ind,
      net_revenue_impact_underpayment_pmt_act.dw_last_update_date_time,
      net_revenue_impact_underpayment_pmt_act.source_system_code
    FROM
      {{ params.param_pbs_core_dataset_name }}.net_revenue_impact_underpayment_pmt_act
  ;
