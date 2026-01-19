-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ref_cc_cers_wf_task_merge.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  cers_wf_task_id BIGNUMERIC(38) NOT NULL,
  cers_term_id BIGNUMERIC(38) NOT NULL,
  ce_wf_profile_id BIGNUMERIC(38) NOT NULL,
  ce_wf_profile_name STRING NOT NULL,
  cers_task_sequence BIGNUMERIC(38) NOT NULL,
  ce_wf_task_id BIGNUMERIC(38),
  cers_wf_task_name STRING,
  cers_wf_task_desc STRING,
  cers_wf_task_expected_dur_rate BIGNUMERIC(38),
  cers_wf_task_is_atch_reqr_ind STRING NOT NULL,
  cers_wf_task_start_date DATE NOT NULL,
  cers_wf_task_completed_date DATE,
  cers_wf_task_is_active_ind STRING NOT NULL,
  cers_wf_task_comments STRING,
  cers_wf_task_is_current_ind STRING NOT NULL,
  cers_wf_task_create_uid BIGNUMERIC(38) NOT NULL,
  cers_wf_task_create_date DATE NOT NULL,
  cers_wf_task_completed_by_uid BIGNUMERIC(38),
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY company_code, coid, cers_wf_task_id;
