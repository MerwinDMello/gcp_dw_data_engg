-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_insurance AS SELECT
    patient_insurance.patient_dw_id,
    patient_insurance.insurance_order_num,
    patient_insurance.payor_dw_id,
    patient_insurance.group_name,
    patient_insurance.group_num,
    patient_insurance.coid,
    patient_insurance.company_code,
    patient_insurance.iplan_id,
    patient_insurance.financial_class_code,
    patient_insurance.hic_claim_num,
    patient_insurance.claim_submit_date,
    patient_insurance.denial_code,
    patient_insurance.denial_status_code,
    patient_insurance.payor_cont_auto_post_ind,
    patient_insurance.mail_to_name,
    patient_insurance.address_dw_id,
    patient_insurance.log_id,
    patient_insurance.allowance_amt,
    patient_insurance.adj_amt,
    patient_insurance.payor_balance_amt,
    patient_insurance.last_pmt_rcvd_amt,
    patient_insurance.last_pmt_rcvd_date,
    patient_insurance.total_pmt_rcvd_amt,
    patient_insurance.gross_reimbursement_amt,
    patient_insurance.gross_reimbursement_var_amt,
    patient_insurance.payor_liability_amt,
    patient_insurance.source_system_code,
    patient_insurance.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.patient_insurance
;
