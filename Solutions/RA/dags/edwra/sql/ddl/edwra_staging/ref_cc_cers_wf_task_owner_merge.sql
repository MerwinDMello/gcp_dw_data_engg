-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/ref_cc_cers_wf_task_owner_merge.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  cers_wf_task_owner_id BIGNUMERIC(38) NOT NULL,
  cers_wf_task_id BIGNUMERIC(38) NOT NULL,
  cers_wf_task_owner_uid BIGNUMERIC(38),
  cers_wf_task_owner_name STRING,
  cers_wf_task_owner_email_addr STRING,
  cers_wf_task_owner_cplt_ind STRING NOT NULL,
  cers_wf_task_owner_is_notf_ind STRING NOT NULL,
  cers_wf_task_ownr_cplt_date DATE,
  cers_wf_task_ownr_chck_off_ind STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY company_code, coid, cers_wf_task_owner_id;
