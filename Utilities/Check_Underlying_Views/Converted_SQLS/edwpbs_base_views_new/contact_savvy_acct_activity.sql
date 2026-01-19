-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_acct_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_acct_activity AS SELECT
    contact_savvy_acct_activity.acct_activity_id,
    contact_savvy_acct_activity.user_on_acct_date_time,
    contact_savvy_acct_activity.user_left_acct_time,
    contact_savvy_acct_activity.artiva_instance_code,
    contact_savvy_acct_activity.acct_activity_key_num,
    contact_savvy_acct_activity.month_id,
    contact_savvy_acct_activity.patient_dw_id,
    contact_savvy_acct_activity.dollar_strf_sid,
    contact_savvy_acct_activity.payor_dw_id,
    contact_savvy_acct_activity.acct_liability_pool_num,
    contact_savvy_acct_activity.liability_financial_class_code,
    contact_savvy_acct_activity.company_code,
    contact_savvy_acct_activity.coid,
    contact_savvy_acct_activity.unit_num,
    contact_savvy_acct_activity.pat_acct_num,
    contact_savvy_acct_activity.liability_sequence_num,
    contact_savvy_acct_activity.iplan_id,
    contact_savvy_acct_activity.artiva_major_payor_group_code,
    contact_savvy_acct_activity.patient_liability_amt,
    contact_savvy_acct_activity.total_billed_charge_amt,
    contact_savvy_acct_activity.collection_action_cnt,
    contact_savvy_acct_activity.call_hold_time_scnd_amt,
    contact_savvy_acct_activity.acct_worked_ind,
    contact_savvy_acct_activity.user_on_acct_id,
    contact_savvy_acct_activity.time_not_worked_mn_amt,
    contact_savvy_acct_activity.time_worked_mn_amt,
    contact_savvy_acct_activity.time_work_on_hold_mn_amt,
    contact_savvy_acct_activity.user_log_acct_time,
    contact_savvy_acct_activity.web_status_code,
    contact_savvy_acct_activity.pool_dialer_type_code,
    contact_savvy_acct_activity.pool_assignment_id,
    contact_savvy_acct_activity.source_system_code,
    contact_savvy_acct_activity.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.contact_savvy_acct_activity
;
