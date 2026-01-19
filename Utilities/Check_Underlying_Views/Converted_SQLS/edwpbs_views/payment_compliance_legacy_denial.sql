-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_legacy_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_legacy_denial
   OPTIONS(description='Legacy SSC denials (open & closed) for corporate payment compliance reporting and reconciliation')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_id,
      a.iplan_insurance_order_num,
      a.appeal_level_num,
      a.reporting_date,
      a.coid,
      a.company_code,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.unit_num,
      CASE
         a.appeal_disp_code
        WHEN '' THEN 0
        ELSE CAST(a.appeal_disp_code as INT64)
      END AS appeal_disp_code,
      a.denial_code,
      a.web_disp_type_code,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(a.acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS acct_bal_amt,
      ROUND(a.payor_bal_amt, 3, 'ROUND_HALF_EVEN') AS payor_bal_amt,
      a.appeal_root_cause_sid,
      ROUND(a.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      ROUND(a.appeal_crnt_bal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_crnt_bal_amt,
      ROUND(a.overturned_acct_amt, 3, 'ROUND_HALF_EVEN') AS overturned_acct_amt,
      ROUND(a.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      ROUND(a.xfer_next_party_amt, 3, 'ROUND_HALF_EVEN') AS xfer_next_party_amt,
      ROUND(a.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      a.denial_date,
      a.appeal_level_origination_date_time,
      a.appeal_deadline_date,
      a.appeal_closing_date,
      a.work_again_date,
      a.appeal_origination_date,
      a.appeal_assigned_user_id,
      a.pa_vendor_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_legacy_denial AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
