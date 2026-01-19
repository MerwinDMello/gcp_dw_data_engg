-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_legacy_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_legacy_denial AS SELECT
    a.patient_dw_id,
    a.iplan_id,
    a.iplan_insurance_order_num,
    a.appeal_level_num,
    a.reporting_date,
    a.coid,
    a.company_code,
    a.pat_acct_num,
    a.unit_num,
    CAST(/* expression of unknown or erroneous type */ a.appeal_disp_code as INT64) AS appeal_disp_code,
    a.denial_code,
    a.web_disp_type_code,
    a.total_charge_amt,
    a.acct_bal_amt,
    a.payor_bal_amt,
    a.appeal_root_cause_sid,
    a.appeal_amt,
    a.appeal_crnt_bal_amt,
    a.overturned_acct_amt,
    a.write_off_denial_account_amt,
    a.xfer_next_party_amt,
    a.denied_charge_amt,
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
