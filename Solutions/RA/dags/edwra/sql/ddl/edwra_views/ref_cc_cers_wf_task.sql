-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_cers_wf_task.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_cers_wf_task
   OPTIONS(description='Reference table to calculation engine rate schedule workflow tasks.')
  AS SELECT
      a.company_code,
      a.coid,
      a.cers_wf_task_id,
      a.cers_term_id,
      a.ce_wf_profile_id,
      a.ce_wf_profile_name,
      a.cers_task_sequence,
      a.ce_wf_task_id,
      a.cers_wf_task_name,
      a.cers_wf_task_desc,
      a.cers_wf_task_expected_dur_rate,
      a.cers_wf_task_is_atch_reqr_ind,
      a.cers_wf_task_start_date,
      a.cers_wf_task_completed_date,
      a.cers_wf_task_is_active_ind,
      a.cers_wf_task_comments,
      a.cers_wf_task_is_current_ind,
      a.cers_wf_task_create_uid,
      a.cers_wf_task_create_date,
      a.cers_wf_task_completed_by_uid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_cers_wf_task AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
