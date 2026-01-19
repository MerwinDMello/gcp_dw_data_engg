-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.cc_account_activity AS SELECT
    ROUND(bv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(bv.cc_activity_id, 0, 'ROUND_HALF_EVEN') AS cc_activity_id,
    bv.company_code,
    bv.coid,
    ROUND(bv.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    bv.iplan_insurance_order_num,
    ROUND(bv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    bv.iplan_id,
    bv.activity_create_date_time,
    bv.activity_update_date_time,
    bv.create_user_id,
    bv.activity_subject_desc,
    bv.activity_desc,
    bv.expected_duration_num,
    bv.activity_due_date,
    bv.activity_owner_user_id,
    bv.create_denial_collect_ind,
    ROUND(bv.activity_type_id, 0, 'ROUND_HALF_EVEN') AS activity_type_id,
    ROUND(bv.activity_status_id, 0, 'ROUND_HALF_EVEN') AS activity_status_id,
    bv.activity_complete_date_time,
    bv.complete_user_id,
    bv.activity_resolution_desc,
    bv.activity_source_code,
    bv.active_ind,
    bv.dw_last_update_date_time,
    bv.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.cc_account_activity AS bv
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(bv.coid) = upper(sf.co_id)
     AND sf.user_id = session_user()
;
