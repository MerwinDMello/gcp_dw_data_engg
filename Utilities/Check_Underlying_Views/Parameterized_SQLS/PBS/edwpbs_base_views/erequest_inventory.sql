-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/erequest_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.erequest_inventory
   OPTIONS(description='Daily Inventory of ERequest information to monitor the billing cycle.')
  AS SELECT
      erequest_inventory.rptg_date,
      erequest_inventory.erequest_id,
      erequest_inventory.claim_id,
      erequest_inventory.coid,
      erequest_inventory.erequest_user_department_num,
      ROUND(erequest_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(erequest_inventory.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      erequest_inventory.iplan_id,
      ROUND(erequest_inventory.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(erequest_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      erequest_inventory.patient_type_code_pos1,
      erequest_inventory.company_code,
      erequest_inventory.discharge_date,
      erequest_inventory.admission_date,
      erequest_inventory.bill_type_code,
      erequest_inventory.med_rec_num,
      erequest_inventory.account_status_sid,
      erequest_inventory.vendor_id,
      erequest_inventory.final_queue_dept_id,
      erequest_inventory.current_queue_dept_id,
      erequest_inventory.previous_queue_dept_id,
      erequest_inventory.queue_changed_ind,
      erequest_inventory.provider_npi,
      erequest_inventory.unbilled_reason_code,
      ROUND(erequest_inventory.claim_charge_amt, 3, 'ROUND_HALF_EVEN') AS claim_charge_amt,
      ROUND(erequest_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(erequest_inventory.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      erequest_inventory.request_type_id,
      erequest_inventory.request_created_by_user_id,
      erequest_inventory.request_status_code,
      erequest_inventory.concuity_acct_ind,
      erequest_inventory.payer_claim_control_id,
      erequest_inventory.service_code,
      erequest_inventory.service_from_date,
      erequest_inventory.service_to_date,
      erequest_inventory.request_created_date,
      erequest_inventory.last_activity_date,
      erequest_inventory.last_activity_by_user_id,
      erequest_inventory.last_queue_change_date,
      erequest_inventory.final_queue_change_date,
      erequest_inventory.final_queue_changed_by_user_id,
      erequest_inventory.escalation_override_date,
      erequest_inventory.claim_submit_date,
      erequest_inventory.claim_download_date,
      erequest_inventory.erequest_src_sys_sid,
      erequest_inventory.purge_ind,
      erequest_inventory.new_req_ind,
      erequest_inventory.updated_req_ind,
      erequest_inventory.final_diagnosis_flg,
      erequest_inventory.source_system_code,
      erequest_inventory.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.erequest_inventory
  ;
