-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_savvy_acct_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.contact_savvy_acct_activity AS SELECT
    a.acct_activity_id,
    a.user_on_acct_date_time,
    a.user_left_acct_time,
    a.artiva_instance_code,
    a.acct_activity_key_num,
    a.month_id,
    a.patient_dw_id,
    a.dollar_strf_sid,
    a.payor_dw_id,
    a.acct_liability_pool_num,
    a.liability_financial_class_code,
    a.company_code,
    a.coid,
    a.unit_num,
    a.pat_acct_num,
    a.liability_sequence_num,
    a.iplan_id,
    a.artiva_major_payor_group_code,
    a.patient_liability_amt,
    a.total_billed_charge_amt,
    a.collection_action_cnt,
    a.call_hold_time_scnd_amt,
    a.acct_worked_ind,
    a.user_on_acct_id,
    a.time_not_worked_mn_amt,
    a.time_worked_mn_amt,
    a.time_work_on_hold_mn_amt,
    a.user_log_acct_time,
    a.web_status_code,
    a.pool_dialer_type_code,
    a.pool_assignment_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_acct_activity AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
