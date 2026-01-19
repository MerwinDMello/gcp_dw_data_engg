-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/cc_discrepancy_ra.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.cc_discrepancy_ra AS SELECT
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_order_num,
    a.extract_date_time,
    ROUND(a.ar_amt, 3, 'ROUND_HALF_EVEN') AS ar_amt,
    a.actv_desc,
    a.actv_due_date,
    a.activity_kpi_name,
    a.actv_owner_desc,
    a.actv_subject_desc,
    a.admit_source_cd,
    ROUND(a.attending_physician_id, 0, 'ROUND_HALF_EVEN') AS attending_physician_id,
    a.attending_physician_name,
    a.authorization_cd,
    a.billing_contact_name,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.billing_name
    END AS billing_name,
    a.billing_status_cd,
    a.calc_date,
    a.cancel_bill_ind,
    a.cc_pat_type_cd,
    a.coid,
    ROUND(a.collection_amt, 3, 'ROUND_HALF_EVEN') AS collection_amt,
    a.collection_date,
    a.company_code,
    a.comparison_method_desc,
    a.credit_balance_age_desc,
    a.credit_category_name,
    a.crt_placed_activity_date,
    a.discharge_date,
    a.discrepancy_group_name,
    a.first_actv_create_date,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.iplan_id,
    a.insurance_provider_name,
    a.last_actv_completion_age_num,
    a.last_actv_completion_date,
    a.last_owner_change_date,
    a.last_project_change_date,
    a.last_reason_change_date,
    a.last_reason_change_date_2,
    a.last_reason_change_date_3,
    a.last_reason_change_date_4,
    a.last_status_change_date,
    a.max_type_5_trans_date,
    a.model_issue_desc,
    ROUND(a.monbot_acct_payer_id, 0, 'ROUND_HALF_EVEN') AS monbot_acct_payer_id,
    a.non_fin_dcrp_age_num,
    a.non_fin_dcrp_date,
    a.overpayment_age_num,
    a.overpayment_date,
    a.pa_acct_status_desc,
    a.pa_actual_los_num,
    a.pa_discharge_status_cd,
    a.pa_drg_cd,
    a.pa_financial_class_num,
    a.pa_pat_type_desc,
    a.pa_service_cd,
    ROUND(a.pa_total_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS pa_total_acct_bal_amt,
    a.pat_birth_date,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.pat_name
    END AS pat_name,
    a.payor_category_name,
    ROUND(a.payor_due_amt, 3, 'ROUND_HALF_EVEN') AS payor_due_amt,
    ROUND(a.payor_financial_class_id, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_id,
    a.payor_group_name,
    a.payor_pat_cd,
    a.project_name,
    a.rate_schedule_name,
    a.rate_schedule_eff_begin_date,
    a.rate_schedule_eff_end_date,
    a.reason_code,
    a.reason_code_2,
    a.reason_code_3,
    a.reason_code_4,
    a.remit_drg_code,
    ROUND(a.sec_establishment_id, 0, 'ROUND_HALF_EVEN') AS sec_establishment_id,
    a.service_begin_date,
    a.ssc_name,
    a.status_category_desc,
    a.status_desc,
    a.status_kpi_name,
    a.status_phase_desc,
    a.stratification_group_name,
    ROUND(a.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
    ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
    ROUND(a.total_denial_amt, 3, 'ROUND_HALF_EVEN') AS total_denial_amt,
    ROUND(a.total_expected_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_expected_adjustment_amt,
    ROUND(a.total_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_expected_payment_amt,
    ROUND(a.total_pat_resp_amt, 3, 'ROUND_HALF_EVEN') AS total_pat_resp_amt,
    ROUND(a.total_pmt_amt, 3, 'ROUND_HALF_EVEN') AS total_pmt_amt,
    ROUND(a.total_var_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_var_adjustment_amt,
    a.underpayment_age_num,
    a.underpayment_date,
    a.unit_num,
    a.user_completed_actv_age_num,
    a.user_completed_actv_date,
    a.user_completed_actv_desc,
    a.user_completed_actv_ownr_34_id,
    a.user_completed_actv_subj_desc,
    a.var_creation_age_num,
    a.var_creation_date,
    a.var_resolution_age_num,
    a.var_resolution_date,
    a.valid_over_pmt_actv_age_num,
    a.valid_over_pmt_actv_date,
    a.valid_over_pmt_actv_desc,
    a.valid_over_pmt_actv_ownr_desc,
    a.valid_over_pmt_actv_subj_desc,
    a.valid_under_pmt_actv_age_num,
    a.valid_under_pmt_actv_date,
    a.valid_under_pmt_actv_desc,
    a.valid_under_pmt_actv_ownr_id,
    a.valid_under_pmt_actv_subj_desc,
    a.validation_group_name,
    a.work_queue_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.cc_discrepancy_ra AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'PN'
    ) AS pn ON pn.userid = session_user()
;
