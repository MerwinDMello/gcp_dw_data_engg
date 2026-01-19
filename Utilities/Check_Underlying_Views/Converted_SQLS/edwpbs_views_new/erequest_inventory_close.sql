-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/erequest_inventory_close.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.erequest_inventory_close AS SELECT
    a.rptg_date,
    a.erequest_id,
    a.claim_id,
    a.coid,
    a.erequest_user_department_num,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_id,
    a.financial_class_code,
    a.pat_acct_num,
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
    a.claim_charge_amt,
    a.total_account_balance_amt,
    a.total_charge_amt,
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
  WHERE trim(upper(a.request_status_code)) = 'C'
;
