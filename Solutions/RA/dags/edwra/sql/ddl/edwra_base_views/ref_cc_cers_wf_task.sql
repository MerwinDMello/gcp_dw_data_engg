-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_cers_wf_task.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_cers_wf_task
   OPTIONS(description='Reference table to calculation engine rate schedule workflow tasks.')
  AS SELECT
      ref_cc_cers_wf_task.company_code,
      ref_cc_cers_wf_task.coid,
      ref_cc_cers_wf_task.cers_wf_task_id,
      ref_cc_cers_wf_task.cers_term_id,
      ref_cc_cers_wf_task.ce_wf_profile_id,
      ref_cc_cers_wf_task.ce_wf_profile_name,
      ref_cc_cers_wf_task.cers_task_sequence,
      ref_cc_cers_wf_task.ce_wf_task_id,
      ref_cc_cers_wf_task.cers_wf_task_name,
      ref_cc_cers_wf_task.cers_wf_task_desc,
      ref_cc_cers_wf_task.cers_wf_task_expected_dur_rate,
      ref_cc_cers_wf_task.cers_wf_task_is_atch_reqr_ind,
      ref_cc_cers_wf_task.cers_wf_task_start_date,
      ref_cc_cers_wf_task.cers_wf_task_completed_date,
      ref_cc_cers_wf_task.cers_wf_task_is_active_ind,
      ref_cc_cers_wf_task.cers_wf_task_comments,
      ref_cc_cers_wf_task.cers_wf_task_is_current_ind,
      ref_cc_cers_wf_task.cers_wf_task_create_uid,
      ref_cc_cers_wf_task.cers_wf_task_create_date,
      ref_cc_cers_wf_task.cers_wf_task_completed_by_uid,
      ref_cc_cers_wf_task.source_system_code,
      ref_cc_cers_wf_task.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task
  ;
