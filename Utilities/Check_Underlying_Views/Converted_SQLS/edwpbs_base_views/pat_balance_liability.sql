-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pat_balance_liability.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.pat_balance_liability
   OPTIONS(description='Daily snapshot of patient balance and liability details from PA to drive SSC operations. This table will be truncated and loaded daily.')
  AS SELECT
      ROUND(pat_balance_liability.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      pat_balance_liability.insurance_order_num,
      ROUND(pat_balance_liability.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      pat_balance_liability.company_code,
      pat_balance_liability.coid,
      pat_balance_liability.unit_num,
      ROUND(pat_balance_liability.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      pat_balance_liability.iplan_identifier,
      ROUND(pat_balance_liability.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(pat_balance_liability.prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_amt,
      ROUND(pat_balance_liability.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
      ROUND(pat_balance_liability.bill_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS bill_adjustment_amt,
      ROUND(pat_balance_liability.allowance_amt, 3, 'ROUND_HALF_EVEN') AS allowance_amt,
      ROUND(pat_balance_liability.nonbill_adj_amt, 3, 'ROUND_HALF_EVEN') AS nonbill_adj_amt,
      ROUND(pat_balance_liability.bal_due_amt, 3, 'ROUND_HALF_EVEN') AS bal_due_amt,
      ROUND(pat_balance_liability.sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS sys_adj_amt,
      pat_balance_liability.source_system_code,
      pat_balance_liability.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.pat_balance_liability
  ;
