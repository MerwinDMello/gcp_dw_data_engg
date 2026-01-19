-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.cc_account_activity AS SELECT
    cc_account_activity.patient_dw_id,
    cc_account_activity.cc_activity_id,
    cc_account_activity.company_code,
    cc_account_activity.coid,
    cc_account_activity.payor_dw_id,
    cc_account_activity.iplan_insurance_order_num,
    cc_account_activity.pat_acct_num,
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
    cc_account_activity.activity_type_id,
    cc_account_activity.activity_status_id,
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
