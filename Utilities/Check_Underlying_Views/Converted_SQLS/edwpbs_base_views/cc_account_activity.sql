-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.cc_account_activity AS SELECT
    ROUND(cc_account_activity.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(cc_account_activity.cc_activity_id, 0, 'ROUND_HALF_EVEN') AS cc_activity_id,
    cc_account_activity.company_code,
    cc_account_activity.coid,
    ROUND(cc_account_activity.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    cc_account_activity.iplan_insurance_order_num,
    ROUND(cc_account_activity.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    cc_account_activity.iplan_id,
    cc_account_activity.activity_create_date_time,
    cc_account_activity.activity_update_date_time,
    cc_account_activity.create_user_id,
    cc_account_activity.activity_subject_desc,
    cc_account_activity.activity_desc,
    cc_account_activity.expected_duration_num,
    cc_account_activity.activity_due_date,
    cc_account_activity.activity_owner_user_id,
    cc_account_activity.create_denial_collect_ind,
    ROUND(cc_account_activity.activity_type_id, 0, 'ROUND_HALF_EVEN') AS activity_type_id,
    ROUND(cc_account_activity.activity_status_id, 0, 'ROUND_HALF_EVEN') AS activity_status_id,
    cc_account_activity.activity_complete_date_time,
    cc_account_activity.complete_user_id,
    cc_account_activity.activity_resolution_desc,
    cc_account_activity.activity_source_code,
    cc_account_activity.active_ind,
    cc_account_activity.dw_last_update_date_time,
    cc_account_activity.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.cc_account_activity
;
