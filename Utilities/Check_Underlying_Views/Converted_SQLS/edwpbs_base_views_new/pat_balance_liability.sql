-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pat_balance_liability.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.pat_balance_liability AS SELECT
    pat_balance_liability.patient_dw_id,
    pat_balance_liability.insurance_order_num,
    pat_balance_liability.payor_dw_id,
    pat_balance_liability.company_code,
    pat_balance_liability.coid,
    pat_balance_liability.unit_num,
    pat_balance_liability.pat_acct_num,
    pat_balance_liability.iplan_identifier,
    pat_balance_liability.total_charge_amt,
    pat_balance_liability.prorated_liability_amt,
    pat_balance_liability.cash_amt,
    pat_balance_liability.bill_adjustment_amt,
    pat_balance_liability.allowance_amt,
    pat_balance_liability.nonbill_adj_amt,
    pat_balance_liability.bal_due_amt,
    pat_balance_liability.sys_adj_amt,
    pat_balance_liability.source_system_code,
    pat_balance_liability.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.pat_balance_liability
;
