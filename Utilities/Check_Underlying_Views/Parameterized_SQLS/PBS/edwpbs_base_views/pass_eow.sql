-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pass_eow.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.pass_eow
   OPTIONS(description='A Patient Account Snapshot at weekly level. To analyze the AR Acccounts and its reimbursement.')
  AS SELECT
      pass_eow.company_code,
      pass_eow.coid,
      pass_eow.reporting_date,
      ROUND(pass_eow.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      pass_eow.log_id,
      pass_eow.iplan_id,
      pass_eow.patient_type_code,
      ROUND(pass_eow.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      pass_eow.unit_num,
      ROUND(pass_eow.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(pass_eow.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      pass_eow.final_bill_date,
      pass_eow.account_status_code,
      ROUND(pass_eow.total_billed_charge_amount, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amount,
      ROUND(pass_eow.eor_gross_reimbursement_amount, 3, 'ROUND_HALF_EVEN') AS eor_gross_reimbursement_amount,
      ROUND(pass_eow.total_account_balance_amount, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amount
    FROM
      {{ params.param_pbs_core_dataset_name }}.pass_eow
  ;
