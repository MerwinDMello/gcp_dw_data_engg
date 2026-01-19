-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/erequest_inventory_close.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.erequest_inventory_close AS SELECT
    a.rptg_date,
    a.erequest_id,
    a.claim_id,
    a.coid,
    a.erequest_user_department_num,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_id,
    ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.patient_type_code_pos1,
    a.company_code,
    a.discharge_date,
    a.admission_date,
    a.bill_type_code,
    a.med_rec_num,
    a.account_status_sid,
    a.vendor_id,
    a.final_queue_dept_id,
    a.current_queue_dept_id,
    a.previous_queue_dept_id,
    a.queue_changed_ind,
    a.provider_npi,
    a.unbilled_reason_code,
    ROUND(a.claim_charge_amt, 3, 'ROUND_HALF_EVEN') AS claim_charge_amt,
    ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
    a.request_type_id,
    a.request_created_by_user_id,
    a.request_status_code,
    a.concuity_acct_ind,
    a.payer_claim_control_id,
    a.service_code,
    a.service_from_date,
    a.service_to_date,
    a.request_created_date,
    a.last_activity_date,
    a.last_activity_by_user_id,
    a.last_queue_change_date,
    a.final_queue_change_date,
    a.final_queue_changed_by_user_id,
    a.escalation_override_date,
    a.claim_submit_date,
    a.claim_download_date,
    a.erequest_src_sys_sid,
    a.purge_ind,
    a.new_req_ind,
    a.updated_req_ind,
    a.final_diagnosis_flg,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.erequest_inventory AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
  WHERE upper(trim(upper(a.request_status_code))) = 'C'
;
