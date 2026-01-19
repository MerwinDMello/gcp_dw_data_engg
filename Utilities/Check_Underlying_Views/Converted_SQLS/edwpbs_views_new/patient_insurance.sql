-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_insurance AS SELECT
    a.patient_dw_id,
    a.insurance_order_num,
    a.payor_dw_id,
    a.group_name,
    a.group_num,
    a.coid,
    a.company_code,
    a.iplan_id,
    a.financial_class_code,
    a.hic_claim_num,
    a.claim_submit_date,
    a.denial_code,
    a.denial_status_code,
    a.payor_cont_auto_post_ind,
    a.mail_to_name,
    a.address_dw_id,
    a.log_id,
    a.allowance_amt,
    a.adj_amt,
    a.payor_balance_amt,
    a.last_pmt_rcvd_amt,
    a.last_pmt_rcvd_date,
    a.total_pmt_rcvd_amt,
    a.gross_reimbursement_amt,
    a.gross_reimbursement_var_amt,
    a.payor_liability_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_insurance AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
