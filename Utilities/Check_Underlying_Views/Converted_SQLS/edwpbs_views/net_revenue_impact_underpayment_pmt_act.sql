-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_underpayment_pmt_act.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.net_revenue_impact_underpayment_pmt_act
   OPTIONS(description='Daily snapshot of Underpayment Recoveries to analyze the impact with Discrepancies raised and finally the impat on Net Revenue.\r\rThis table tracks the payment activity related to underpayment.')
  AS SELECT
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.company_code,
      a.coid,
      a.admission_date,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.iplan_id,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.patient_type_code,
      a.discrepancy_origination_date,
      a.discharge_date,
      ROUND(a.under_payment_activity_amt, 4, 'ROUND_HALF_EVEN') AS under_payment_activity_amt,
      a.payment_discrepancy_ind,
      a.take_back_ind,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_underpayment_pmt_act AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
