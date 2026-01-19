-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_unbilled_inventory
   OPTIONS(description='Daily Inventory of Unbilled accounts to monitor the billing cycle.')
  AS SELECT
      a.rptg_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.claim_id,
      a.request_id,
      a.queue_dept_id,
      a.unbilled_status_code,
      a.unbilled_reason_code,
      a.him_unbilled_reason_code,
      a.acct_type_desc,
      a.request_created_date,
      a.queue_assigned_date,
      a.last_activity_date,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.coid,
      a.company_code,
      a.unit_num,
      a.final_bill_date,
      a.discharge_date,
      a.admission_date,
      a.patient_type_code_pos1,
      a.payor_sid,
      ROUND(a.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      a.iplan_id_ins1,
      a.payor_financial_class_sid,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(a.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(a.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(a.alert_gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS alert_gross_charge_amt,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(a.alert_total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS alert_total_account_balance_amt,
      ROUND(a.rh_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS rh_total_charge_amt,
      a.hold_bill_reason_code,
      a.account_process_ind,
      a.unbilled_responsibility_ind,
      a.claim_submit_date,
      a.final_bill_hold_ind,
      a.bill_type_code,
      a.date_of_claim,
      a.request_file_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_unbilled_inventory AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
