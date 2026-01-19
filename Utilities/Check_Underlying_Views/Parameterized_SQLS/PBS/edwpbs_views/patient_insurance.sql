-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.patient_insurance
   OPTIONS(description='Daily snapshot of  patient Insurance related information  from PA')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.insurance_order_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.group_name,
      a.group_num,
      a.coid,
      a.company_code,
      a.iplan_id,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.hic_claim_num,
      a.claim_submit_date,
      a.denial_code,
      a.denial_status_code,
      a.payor_cont_auto_post_ind,
      a.mail_to_name,
      ROUND(a.address_dw_id, 0, 'ROUND_HALF_EVEN') AS address_dw_id,
      a.log_id,
      ROUND(a.allowance_amt, 3, 'ROUND_HALF_EVEN') AS allowance_amt,
      ROUND(a.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      ROUND(a.payor_balance_amt, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt,
      ROUND(a.last_pmt_rcvd_amt, 3, 'ROUND_HALF_EVEN') AS last_pmt_rcvd_amt,
      a.last_pmt_rcvd_date,
      ROUND(a.total_pmt_rcvd_amt, 3, 'ROUND_HALF_EVEN') AS total_pmt_rcvd_amt,
      ROUND(a.gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS gross_reimbursement_amt,
      ROUND(a.gross_reimbursement_var_amt, 3, 'ROUND_HALF_EVEN') AS gross_reimbursement_var_amt,
      ROUND(a.payor_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_liability_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.patient_insurance AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
