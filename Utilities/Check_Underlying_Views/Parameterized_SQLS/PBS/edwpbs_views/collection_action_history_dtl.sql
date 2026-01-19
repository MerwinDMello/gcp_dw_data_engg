-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_action_history_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.collection_action_history_dtl
   OPTIONS(description='This table maitains history of action results on the collection workflow of an account.')
  AS SELECT
      ROUND(a.activity_history_key_num, 0, 'ROUND_HALF_EVEN') AS activity_history_key_num,
      a.artiva_instance_code,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.reporting_date,
      a.action_date,
      a.action_time,
      a.action_code,
      a.result_code,
      a.action_updt_by_user_id,
      a.action_status_code,
      a.iplan_id,
      a.pool_assignment_id,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.company_code,
      a.coid,
      a.unit_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.collection_action_history_dtl AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
