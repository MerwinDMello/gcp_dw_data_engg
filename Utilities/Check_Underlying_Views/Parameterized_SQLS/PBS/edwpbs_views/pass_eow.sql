-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/pass_eow.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.pass_eow
   OPTIONS(description='A Patient Account Snapshot at weekly level. To analyze the AR Acccounts and its reimbursement.')
  AS SELECT
      a.company_code,
      a.coid,
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.log_id,
      a.iplan_id,
      a.patient_type_code,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.unit_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.final_bill_date,
      a.account_status_code,
      ROUND(a.total_billed_charge_amount, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amount,
      ROUND(a.eor_gross_reimbursement_amount, 3, 'ROUND_HALF_EVEN') AS eor_gross_reimbursement_amount,
      ROUND(a.total_account_balance_amount, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amount
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.pass_eow AS a
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
