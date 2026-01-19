-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_doc_req_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.collection_doc_req_dtl
   OPTIONS(description='Collection Document Request History is maintained in this table.')
  AS SELECT
      ROUND(a.doc_req_key_num, 0, 'ROUND_HALF_EVEN') AS doc_req_key_num,
      a.artiva_instance_code,
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_id,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.status_code,
      ROUND(a.encounter_balance_amt, 3, 'ROUND_HALF_EVEN') AS encounter_balance_amt,
      ROUND(a.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(a.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
      ROUND(a.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(a.current_balance_amt, 3, 'ROUND_HALF_EVEN') AS current_balance_amt,
      a.claim_submit_date,
      a.requested_date,
      a.response_due_date,
      a.approved_date,
      a.denied_date,
      a.create_date,
      a.sent_date,
      a.tracking_dtl_text,
      a.received_date,
      a.review_type_code,
      a.reason_text,
      a.othr_reason_text,
      a.mail_receipt_date,
      a.receipt_cnfm_num,
      a.cover_letter_reqr_ind,
      a.doc_delivery_method_text,
      a.mailed_address_sid,
      a.signed_by_comment_text,
      a.admit_order_ind,
      a.admit_order_aprv_ind,
      a.implant_inv_ind,
      a.implant_inv_aprv_ind,
      a.oth_dtl_ind,
      a.oth_dtl_aprv_ind,
      a.dchg_smry_ind,
      a.dchg_smry_aprv_ind,
      a.emrg_rpt_ind,
      a.emrg_rpt_aprv_ind,
      a.accd_dtl_ind,
      a.accd_dtl_aprv_ind,
      a.itm_bill_ind,
      a.itm_bill_aprv_ind,
      a.itm_bill_req_date,
      a.itm_bill_sent_date,
      a.first_letter_sent_date,
      a.second_letter_sent_date,
      a.third_letter_sent_date,
      a.sent_to_prepay_mc_ind,
      a.prepay_mc_sent_date,
      a.cplt_med_rec_ind,
      a.cplt_med_rec_aprv_ind,
      a.med_rec_req_date,
      a.med_rec_sent_date,
      a.user_id,
      a.user_dept_num,
      a.vendor_id,
      a.vendor_mail_comment_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.collection_doc_req_dtl AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
