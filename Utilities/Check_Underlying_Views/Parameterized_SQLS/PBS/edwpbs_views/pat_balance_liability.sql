-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/pat_balance_liability.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.pat_balance_liability
   OPTIONS(description='Daily snapshot of patient balance and liability details from PA to drive SSC operations. This table will be truncated and loaded daily.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.insurance_order_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.iplan_identifier,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(a.prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_amt,
      ROUND(a.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
      ROUND(a.bill_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS bill_adjustment_amt,
      ROUND(a.allowance_amt, 3, 'ROUND_HALF_EVEN') AS allowance_amt,
      ROUND(a.nonbill_adj_amt, 3, 'ROUND_HALF_EVEN') AS nonbill_adj_amt,
      ROUND(a.bal_due_amt, 3, 'ROUND_HALF_EVEN') AS bal_due_amt,
      ROUND(a.sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS sys_adj_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.pat_balance_liability AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
