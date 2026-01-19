-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_acct_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.contact_savvy_acct_activity
   OPTIONS(description='Account Activity Information by Persistent Time from Contact Savvy Application Add-On to Artiva system')
  AS SELECT
      ROUND(contact_savvy_acct_activity.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      contact_savvy_acct_activity.user_on_acct_date_time,
      contact_savvy_acct_activity.user_left_acct_time,
      contact_savvy_acct_activity.artiva_instance_code,
      ROUND(contact_savvy_acct_activity.acct_activity_key_num, 0, 'ROUND_HALF_EVEN') AS acct_activity_key_num,
      contact_savvy_acct_activity.month_id,
      ROUND(contact_savvy_acct_activity.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      contact_savvy_acct_activity.dollar_strf_sid,
      ROUND(contact_savvy_acct_activity.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      contact_savvy_acct_activity.acct_liability_pool_num,
      ROUND(contact_savvy_acct_activity.liability_financial_class_code, 0, 'ROUND_HALF_EVEN') AS liability_financial_class_code,
      contact_savvy_acct_activity.company_code,
      contact_savvy_acct_activity.coid,
      contact_savvy_acct_activity.unit_num,
      ROUND(contact_savvy_acct_activity.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      contact_savvy_acct_activity.liability_sequence_num,
      contact_savvy_acct_activity.iplan_id,
      contact_savvy_acct_activity.artiva_major_payor_group_code,
      ROUND(contact_savvy_acct_activity.patient_liability_amt, 3, 'ROUND_HALF_EVEN') AS patient_liability_amt,
      ROUND(contact_savvy_acct_activity.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(contact_savvy_acct_activity.collection_action_cnt, 0, 'ROUND_HALF_EVEN') AS collection_action_cnt,
      contact_savvy_acct_activity.call_hold_time_scnd_amt,
      contact_savvy_acct_activity.acct_worked_ind,
      contact_savvy_acct_activity.user_on_acct_id,
      ROUND(contact_savvy_acct_activity.time_not_worked_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_not_worked_mn_amt,
      ROUND(contact_savvy_acct_activity.time_worked_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_worked_mn_amt,
      ROUND(contact_savvy_acct_activity.time_work_on_hold_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_work_on_hold_mn_amt,
      contact_savvy_acct_activity.user_log_acct_time,
      contact_savvy_acct_activity.web_status_code,
      contact_savvy_acct_activity.pool_dialer_type_code,
      contact_savvy_acct_activity.pool_assignment_id,
      contact_savvy_acct_activity.source_system_code,
      contact_savvy_acct_activity.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.contact_savvy_acct_activity
  ;
