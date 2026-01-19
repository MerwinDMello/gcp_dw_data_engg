-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_action_history_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_action_history_dtl
   OPTIONS(description='This table maitains history of action results on the collection workflow of an account.')
  AS SELECT
      ROUND(collection_action_history_dtl.activity_history_key_num, 0, 'ROUND_HALF_EVEN') AS activity_history_key_num,
      collection_action_history_dtl.artiva_instance_code,
      ROUND(collection_action_history_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_action_history_dtl.reporting_date,
      collection_action_history_dtl.action_date,
      collection_action_history_dtl.action_time,
      collection_action_history_dtl.action_code,
      collection_action_history_dtl.result_code,
      collection_action_history_dtl.action_updt_by_user_id,
      collection_action_history_dtl.action_status_code,
      collection_action_history_dtl.iplan_id,
      collection_action_history_dtl.pool_assignment_id,
      ROUND(collection_action_history_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_action_history_dtl.company_code,
      collection_action_history_dtl.coid,
      collection_action_history_dtl.unit_num,
      collection_action_history_dtl.source_system_code,
      collection_action_history_dtl.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.collection_action_history_dtl
  ;
