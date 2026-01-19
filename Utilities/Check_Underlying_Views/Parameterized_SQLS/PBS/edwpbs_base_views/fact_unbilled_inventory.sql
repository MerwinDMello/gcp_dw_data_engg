-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.fact_unbilled_inventory
   OPTIONS(description='Daily Inventory of Unbilled accounts to monitor the billing cycle.')
  AS SELECT
      fact_unbilled_inventory.rptg_date,
      ROUND(fact_unbilled_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_unbilled_inventory.claim_id,
      fact_unbilled_inventory.request_id,
      fact_unbilled_inventory.queue_dept_id,
      fact_unbilled_inventory.unbilled_status_code,
      fact_unbilled_inventory.unbilled_reason_code,
      fact_unbilled_inventory.him_unbilled_reason_code,
      fact_unbilled_inventory.acct_type_desc,
      fact_unbilled_inventory.request_created_date,
      fact_unbilled_inventory.queue_assigned_date,
      fact_unbilled_inventory.last_activity_date,
      ROUND(fact_unbilled_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_unbilled_inventory.coid,
      fact_unbilled_inventory.company_code,
      fact_unbilled_inventory.unit_num,
      fact_unbilled_inventory.final_bill_date,
      fact_unbilled_inventory.discharge_date,
      fact_unbilled_inventory.admission_date,
      fact_unbilled_inventory.patient_type_code_pos1,
      fact_unbilled_inventory.payor_sid,
      ROUND(fact_unbilled_inventory.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      fact_unbilled_inventory.iplan_id_ins1,
      fact_unbilled_inventory.payor_financial_class_sid,
      ROUND(fact_unbilled_inventory.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(fact_unbilled_inventory.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(fact_unbilled_inventory.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(fact_unbilled_inventory.alert_gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS alert_gross_charge_amt,
      ROUND(fact_unbilled_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(fact_unbilled_inventory.alert_total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS alert_total_account_balance_amt,
      ROUND(fact_unbilled_inventory.rh_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS rh_total_charge_amt,
      fact_unbilled_inventory.hold_bill_reason_code,
      fact_unbilled_inventory.account_process_ind,
      fact_unbilled_inventory.unbilled_responsibility_ind,
      fact_unbilled_inventory.claim_submit_date,
      fact_unbilled_inventory.final_bill_hold_ind,
      fact_unbilled_inventory.bill_type_code,
      fact_unbilled_inventory.date_of_claim,
      fact_unbilled_inventory.request_file_id,
      fact_unbilled_inventory.source_system_code,
      fact_unbilled_inventory.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.fact_unbilled_inventory
  ;
