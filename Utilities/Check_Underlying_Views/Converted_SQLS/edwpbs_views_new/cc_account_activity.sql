-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/cc_account_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.cc_account_activity AS SELECT
    bv.patient_dw_id,
    bv.cc_activity_id,
    bv.company_code,
    bv.coid,
    bv.payor_dw_id,
    bv.iplan_insurance_order_num,
    bv.pat_acct_num,
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
    bv.activity_type_id,
    bv.activity_status_id,
    bv.activity_complete_date_time,
    bv.complete_user_id,
    bv.activity_resolution_desc,
    bv.activity_source_code,
    bv.active_ind,
    bv.dw_last_update_date_time,
    bv.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.cc_account_activity AS bv
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON bv.coid = sf.co_id
     AND sf.user_id = session_user()
;
