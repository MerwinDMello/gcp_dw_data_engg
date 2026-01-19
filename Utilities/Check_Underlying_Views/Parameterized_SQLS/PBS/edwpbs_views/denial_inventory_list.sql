-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/denial_inventory_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.denial_inventory_list
   OPTIONS(description='This table contains all accounts being worked on in the Denial escalation Review tool ( abbreviated as DELR) both Master and Special List')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.denial_escalation_unique_id,
      a.iplan_id,
      a.iplan_insurance_order_num,
      a.month_id,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.project_date,
      a.automated_letter_sent_date_time,
      ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(a.total_acct_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_acct_payment_amt,
      a.acct_closed_ind,
      a.acct_closed_date,
      a.acct_closed_reason_txt,
      a.assigned_attorney_name,
      a.attornery_letter_date,
      a.attornery_assigned_date,
      a.attorney_status_code,
      a.attorney_status_date,
      a.claim_ref_num,
      a.rebilled_ind,
      a.rebill_date,
      a.eligible_for_rebill_ind,
      a.different_iplan_review_ind,
      a.inventory_type_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.denial_inventory_list AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
