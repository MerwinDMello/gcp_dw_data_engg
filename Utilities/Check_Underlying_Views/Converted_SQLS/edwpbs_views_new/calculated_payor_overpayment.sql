-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/calculated_payor_overpayment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.calculated_payor_overpayment AS SELECT
    a.patient_dw_id,
    a.rptg_date,
    a.financial_class_code,
    a.iplan_id,
    a.coid,
    a.company_code,
    a.unit_num,
    a.iplan_insurance_order_num,
    a.payor_dw_id,
    a.pat_acct_num,
    a.patient_person_dw_id,
    a.discrepancy_reason_code_1,
    a.discrepancy_reason_code_3,
    a.cc_project_id,
    a.source_sid,
    a.late_charge_ind,
    a.drg_change_ind,
    a.multiple_pmt_ind,
    a.single_pmt_greater_than_total_chg_ind,
    a.overpayment_metric_sid,
    a.potential_overpayment_amt,
    a.total_account_balance_amt,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.calculated_payor_overpayment AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
