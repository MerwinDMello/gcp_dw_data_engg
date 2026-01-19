-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_cers_wf_task_owner.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_cers_wf_task_owner
   OPTIONS(description='Reference table to owner of calculation engine rate schedule workflow tasks.')
  AS SELECT
      ref_cc_cers_wf_task_owner.company_code,
      ref_cc_cers_wf_task_owner.coid,
      ref_cc_cers_wf_task_owner.cers_wf_task_owner_id,
      ref_cc_cers_wf_task_owner.cers_wf_task_id,
      ref_cc_cers_wf_task_owner.cers_wf_task_owner_uid,
      ref_cc_cers_wf_task_owner.cers_wf_task_owner_name,
      ref_cc_cers_wf_task_owner.cers_wf_task_owner_email_addr,
      ref_cc_cers_wf_task_owner.cers_wf_task_owner_cplt_ind,
      ref_cc_cers_wf_task_owner.cers_wf_task_owner_is_notf_ind,
      ref_cc_cers_wf_task_owner.cers_wf_task_ownr_cplt_date,
      ref_cc_cers_wf_task_owner.cers_wf_task_ownr_chck_off_ind,
      ref_cc_cers_wf_task_owner.source_system_code,
      ref_cc_cers_wf_task_owner.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner
  ;
