-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_legacy_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_legacy_denial
   OPTIONS(description='Legacy SSC denials (open & closed) for corporate payment compliance reporting and reconciliation')
  AS SELECT
      ROUND(payment_compliance_legacy_denial.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_compliance_legacy_denial.iplan_id,
      payment_compliance_legacy_denial.iplan_insurance_order_num,
      payment_compliance_legacy_denial.appeal_level_num,
      payment_compliance_legacy_denial.reporting_date,
      payment_compliance_legacy_denial.coid,
      payment_compliance_legacy_denial.company_code,
      ROUND(payment_compliance_legacy_denial.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_compliance_legacy_denial.unit_num,
      payment_compliance_legacy_denial.appeal_disp_code,
      payment_compliance_legacy_denial.denial_code,
      payment_compliance_legacy_denial.web_disp_type_code,
      ROUND(payment_compliance_legacy_denial.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(payment_compliance_legacy_denial.acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS acct_bal_amt,
      ROUND(payment_compliance_legacy_denial.payor_bal_amt, 3, 'ROUND_HALF_EVEN') AS payor_bal_amt,
      payment_compliance_legacy_denial.appeal_root_cause_sid,
      ROUND(payment_compliance_legacy_denial.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      ROUND(payment_compliance_legacy_denial.appeal_crnt_bal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_crnt_bal_amt,
      ROUND(payment_compliance_legacy_denial.overturned_acct_amt, 3, 'ROUND_HALF_EVEN') AS overturned_acct_amt,
      ROUND(payment_compliance_legacy_denial.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      ROUND(payment_compliance_legacy_denial.xfer_next_party_amt, 3, 'ROUND_HALF_EVEN') AS xfer_next_party_amt,
      ROUND(payment_compliance_legacy_denial.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      payment_compliance_legacy_denial.denial_date,
      payment_compliance_legacy_denial.appeal_level_origination_date_time,
      payment_compliance_legacy_denial.appeal_deadline_date,
      payment_compliance_legacy_denial.appeal_closing_date,
      payment_compliance_legacy_denial.work_again_date,
      payment_compliance_legacy_denial.appeal_origination_date,
      payment_compliance_legacy_denial.appeal_assigned_user_id,
      payment_compliance_legacy_denial.pa_vendor_code,
      payment_compliance_legacy_denial.source_system_code,
      payment_compliance_legacy_denial.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.payment_compliance_legacy_denial
  ;
