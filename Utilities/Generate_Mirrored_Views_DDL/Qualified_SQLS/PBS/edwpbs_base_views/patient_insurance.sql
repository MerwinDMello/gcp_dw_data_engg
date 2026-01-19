-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.patient_insurance
   OPTIONS(description='Daily snapshot of  patient Insurance related information  from PA')
  AS SELECT
      ROUND(patient_insurance.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      patient_insurance.insurance_order_num,
      ROUND(patient_insurance.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      patient_insurance.group_name,
      patient_insurance.group_num,
      patient_insurance.coid,
      patient_insurance.company_code,
      patient_insurance.iplan_id,
      ROUND(patient_insurance.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      patient_insurance.hic_claim_num,
      patient_insurance.claim_submit_date,
      patient_insurance.denial_code,
      patient_insurance.denial_status_code,
      patient_insurance.payor_cont_auto_post_ind,
      patient_insurance.mail_to_name,
      ROUND(patient_insurance.address_dw_id, 0, 'ROUND_HALF_EVEN') AS address_dw_id,
      patient_insurance.log_id,
      ROUND(patient_insurance.allowance_amt, 3, 'ROUND_HALF_EVEN') AS allowance_amt,
      ROUND(patient_insurance.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      ROUND(patient_insurance.payor_balance_amt, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt,
      ROUND(patient_insurance.last_pmt_rcvd_amt, 3, 'ROUND_HALF_EVEN') AS last_pmt_rcvd_amt,
      patient_insurance.last_pmt_rcvd_date,
      ROUND(patient_insurance.total_pmt_rcvd_amt, 3, 'ROUND_HALF_EVEN') AS total_pmt_rcvd_amt,
      ROUND(patient_insurance.gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS gross_reimbursement_amt,
      ROUND(patient_insurance.gross_reimbursement_var_amt, 3, 'ROUND_HALF_EVEN') AS gross_reimbursement_var_amt,
      ROUND(patient_insurance.payor_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_liability_amt,
      patient_insurance.source_system_code,
      patient_insurance.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.patient_insurance
  ;
