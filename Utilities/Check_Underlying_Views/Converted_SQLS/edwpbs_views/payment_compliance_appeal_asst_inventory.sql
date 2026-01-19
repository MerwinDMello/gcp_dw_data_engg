-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_appeal_asst_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_appeal_asst_inventory
   OPTIONS(description='Denials and Appeals Inventory from Appeal Assistant tool for corporate payment compliance reporting and reconciliation')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.last_appeal_attempt_key_num,
      a.appeal_form_downloaded_flag,
      a.reporting_date,
      a.inventory_date,
      a.appeal_status_desc,
      a.appeal_level_num,
      a.iplan_id,
      a.iplan_insurance_order_num,
      a.major_payor_group_id,
      a.coid,
      a.company_code,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.unit_num,
      a.patient_type_code,
      a.meditech_acct_num,
      a.appeal_disp_desc,
      a.denial_code,
      a.denial_code_group_name,
      a.denial_src_system_code,
      a.service_code,
      a.denial_key_num,
      a.denial_src_disp_desc,
      a.appeal_rule_key_num,
      a.appeal_rule_id,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.patient_full_name
      END AS patient_full_name,
      a.patient_birth_date,
      ROUND(a.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      a.claim_id,
      a.hic_claim_num,
      a.application_name,
      a.appeal_folder_status,
      a.appeal_key_num,
      a.appeal_workflow_comment,
      a.appeal_guid,
      a.appeal_evnt_error_desc,
      a.appeal_evnt_error_reason_desc,
      a.src_appeal_num,
      a.onbase_document_id,
      a.acct_selected_ind,
      a.hpf_acct_ind,
      a.hpf_page_total_num,
      a.acct_sent_to_print_ind,
      a.sent_to_print_page_total_num,
      a.acct_confirmed_print_ind,
      a.confirmed_print_page_total_num,
      a.acct_onbase_upload_ind,
      a.onbase_upload_page_total_num,
      a.hpf_required_ind,
      a.hpf_downloaded_flag,
      a.hpf_page_cnt,
      ROUND(a.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      ROUND(a.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      a.account_processed_cnt,
      a.appeal_level_origination_date_time,
      a.appeal_origination_date,
      a.admission_date,
      a.discharge_date,
      a.onbase_upload_date_time,
      a.sent_to_print_date_time,
      a.confirmed_print_date_time,
      a.appeal_evnt_modified_date_time,
      a.appeal_evnt_type_key_num,
      a.appeal_evnt_log_key_num,
      a.appeal_status_modified_date_time,
      a.appeal_evnt_error_date_time,
      a.appeal_evnt_error_category_name,
      a.appeal_rule_template_name,
      a.template_download_date_time,
      a.hpf_selection_date_time,
      a.hpf_download_date_time,
      a.appeal_form_viewed_date_time,
      a.appeal_form_download_date_time,
      a.appeal_created_by_user_id,
      a.appeal_created_date_time,
      a.appeal_uploaded_date_time,
      a.appeal_uploaded_by_user_id,
      a.template_download_by_user_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_appeal_asst_inventory AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON pn.userid = session_user()
  ;
