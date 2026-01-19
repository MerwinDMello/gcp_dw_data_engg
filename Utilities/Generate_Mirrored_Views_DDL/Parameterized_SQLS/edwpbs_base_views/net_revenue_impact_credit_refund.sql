-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.net_revenue_impact_credit_refund
   OPTIONS(description='Daily Credit Refund data to capture net revenue impact.')
  AS SELECT
      net_revenue_impact_credit_refund.reporting_date,
      ROUND(net_revenue_impact_credit_refund.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_credit_refund.iplan_id,
      net_revenue_impact_credit_refund.company_code,
      net_revenue_impact_credit_refund.coid,
      net_revenue_impact_credit_refund.unit_num,
      ROUND(net_revenue_impact_credit_refund.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(net_revenue_impact_credit_refund.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_credit_refund.admission_date,
      net_revenue_impact_credit_refund.discharge_date,
      net_revenue_impact_credit_refund.entered_date,
      net_revenue_impact_credit_refund.patient_type_code,
      ROUND(net_revenue_impact_credit_refund.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_credit_refund.payment_discrepancy_ind,
      net_revenue_impact_credit_refund.cm_dcrp_rslvd_ind,
      ROUND(net_revenue_impact_credit_refund.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      net_revenue_impact_credit_refund.dw_last_update_date_time,
      net_revenue_impact_credit_refund.source_system_code
    FROM
      {{ params.param_pbs_core_dataset_name }}.net_revenue_impact_credit_refund
  ;
