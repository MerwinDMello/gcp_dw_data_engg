-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_action_history_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_action_history_dtl AS SELECT
    collection_action_history_dtl.activity_history_key_num,
    collection_action_history_dtl.artiva_instance_code,
    collection_action_history_dtl.patient_dw_id,
    collection_action_history_dtl.reporting_date,
    collection_action_history_dtl.action_date,
    collection_action_history_dtl.action_time,
    collection_action_history_dtl.action_code,
    collection_action_history_dtl.result_code,
    collection_action_history_dtl.action_updt_by_user_id,
    collection_action_history_dtl.action_status_code,
    collection_action_history_dtl.iplan_id,
    collection_action_history_dtl.pool_assignment_id,
    collection_action_history_dtl.pat_acct_num,
    collection_action_history_dtl.company_code,
    collection_action_history_dtl.coid,
    collection_action_history_dtl.unit_num,
    collection_action_history_dtl.source_system_code,
    collection_action_history_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.collection_action_history_dtl
;
