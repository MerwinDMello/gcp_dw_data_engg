-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/calculated_payor_overpayment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.calculated_payor_overpayment AS SELECT
    calculated_payor_overpayment.patient_dw_id,
    calculated_payor_overpayment.rptg_date,
    calculated_payor_overpayment.financial_class_code,
    calculated_payor_overpayment.iplan_id,
    calculated_payor_overpayment.coid,
    calculated_payor_overpayment.company_code,
    calculated_payor_overpayment.unit_num,
    calculated_payor_overpayment.iplan_insurance_order_num,
    calculated_payor_overpayment.payor_dw_id,
    calculated_payor_overpayment.pat_acct_num,
    calculated_payor_overpayment.patient_person_dw_id,
    calculated_payor_overpayment.discrepancy_reason_code_1,
    calculated_payor_overpayment.discrepancy_reason_code_3,
    calculated_payor_overpayment.cc_project_id,
    calculated_payor_overpayment.source_sid,
    calculated_payor_overpayment.late_charge_ind,
    calculated_payor_overpayment.drg_change_ind,
    calculated_payor_overpayment.multiple_pmt_ind,
    calculated_payor_overpayment.single_pmt_greater_than_total_chg_ind,
    calculated_payor_overpayment.overpayment_metric_sid,
    calculated_payor_overpayment.potential_overpayment_amt,
    calculated_payor_overpayment.total_account_balance_amt,
    calculated_payor_overpayment.source_system_code,
    calculated_payor_overpayment.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.calculated_payor_overpayment
;
