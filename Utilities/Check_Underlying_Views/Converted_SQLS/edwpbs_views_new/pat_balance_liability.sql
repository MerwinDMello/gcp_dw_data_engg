-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/pat_balance_liability.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.pat_balance_liability AS SELECT
    a.patient_dw_id,
    a.insurance_order_num,
    a.payor_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.pat_acct_num,
    a.iplan_identifier,
    a.total_charge_amt,
    a.prorated_liability_amt,
    a.cash_amt,
    a.bill_adjustment_amt,
    a.allowance_amt,
    a.nonbill_adj_amt,
    a.bal_due_amt,
    a.sys_adj_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.pat_balance_liability AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.coid = b.co_id
     AND b.user_id = session_user()
;
