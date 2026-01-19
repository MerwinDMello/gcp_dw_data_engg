-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/denial_inventory_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.denial_inventory_list
   OPTIONS(description='This table contains all accounts being worked on in the Denial escalation Review tool ( abbreviated as DELR) both Master and Special List')
  AS SELECT
      ROUND(denial_inventory_list.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      denial_inventory_list.denial_escalation_unique_id,
      denial_inventory_list.iplan_id,
      denial_inventory_list.iplan_insurance_order_num,
      denial_inventory_list.month_id,
      denial_inventory_list.company_code,
      denial_inventory_list.coid,
      denial_inventory_list.unit_num,
      ROUND(denial_inventory_list.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      denial_inventory_list.project_date,
      denial_inventory_list.automated_letter_sent_date_time,
      ROUND(denial_inventory_list.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(denial_inventory_list.total_acct_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_acct_payment_amt,
      denial_inventory_list.acct_closed_ind,
      denial_inventory_list.acct_closed_date,
      denial_inventory_list.acct_closed_reason_txt,
      denial_inventory_list.assigned_attorney_name,
      denial_inventory_list.attornery_letter_date,
      denial_inventory_list.attornery_assigned_date,
      denial_inventory_list.attorney_status_code,
      denial_inventory_list.attorney_status_date,
      denial_inventory_list.claim_ref_num,
      denial_inventory_list.rebilled_ind,
      denial_inventory_list.rebill_date,
      denial_inventory_list.eligible_for_rebill_ind,
      denial_inventory_list.different_iplan_review_ind,
      denial_inventory_list.inventory_type_name,
      denial_inventory_list.source_system_code,
      denial_inventory_list.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.denial_inventory_list
  ;
