-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_savvy_acct_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.contact_savvy_acct_activity
   OPTIONS(description='Account Activity Information by Persistent Time from Contact Savvy Application Add-On to Artiva system')
  AS SELECT
      ROUND(a.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      a.user_on_acct_date_time,
      a.user_left_acct_time,
      a.artiva_instance_code,
      ROUND(a.acct_activity_key_num, 0, 'ROUND_HALF_EVEN') AS acct_activity_key_num,
      a.month_id,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.dollar_strf_sid,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.acct_liability_pool_num,
      ROUND(a.liability_financial_class_code, 0, 'ROUND_HALF_EVEN') AS liability_financial_class_code,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.liability_sequence_num,
      a.iplan_id,
      a.artiva_major_payor_group_code,
      ROUND(a.patient_liability_amt, 3, 'ROUND_HALF_EVEN') AS patient_liability_amt,
      ROUND(a.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(a.collection_action_cnt, 0, 'ROUND_HALF_EVEN') AS collection_action_cnt,
      a.call_hold_time_scnd_amt,
      a.acct_worked_ind,
      a.user_on_acct_id,
      ROUND(a.time_not_worked_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_not_worked_mn_amt,
      ROUND(a.time_worked_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_worked_mn_amt,
      ROUND(a.time_work_on_hold_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_work_on_hold_mn_amt,
      a.user_log_acct_time,
      a.web_status_code,
      a.pool_dialer_type_code,
      a.pool_assignment_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_acct_activity AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
