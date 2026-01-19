-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_cers_wf_task.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  cers_wf_task_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Primary key for each corresponding EDWRA_STAGING.Cers_Workflow_Task row.'),
  cers_term_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Foreign Key to cers_term.id'),
  ce_wf_profile_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Foreign Key to ce_workflow_profile.id'),
  ce_wf_profile_name STRING NOT NULL OPTIONS(description='Calculation engine workflow profile name from ce_workflow_profile.name via ce_workflow_profile.id = ce_workflow_task_id.'),
  cers_task_sequence BIGNUMERIC(38) NOT NULL OPTIONS(description='Sequence number for each task within the Calculation Engine Rate Schedule Workflow.'),
  ce_wf_task_id BIGNUMERIC(38) OPTIONS(description='Foreign Key to ce_workflow_task.id'),
  cers_wf_task_name STRING OPTIONS(description='Name of Calculation Engine Rate Schedule Workflow Task.'),
  cers_wf_task_desc STRING OPTIONS(description='Description of Calculation Engine Rate Schedule Workflow Task.'),
  cers_wf_task_expected_dur_rate BIGNUMERIC(38) OPTIONS(description='Number of days until task is expected to complete.'),
  cers_wf_task_is_atch_reqr_ind STRING NOT NULL OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  cers_wf_task_start_date DATE NOT NULL OPTIONS(description='The calendar date work on the task started'),
  cers_wf_task_completed_date DATE OPTIONS(description='The calendar date and time work on the task completed.'),
  cers_wf_task_is_active_ind STRING NOT NULL OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  cers_wf_task_comments STRING OPTIONS(description='Comments related to the task.'),
  cers_wf_task_is_current_ind STRING NOT NULL OPTIONS(description='The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  cers_wf_task_create_uid BIGNUMERIC(38) NOT NULL OPTIONS(description='Identification number of the user that created the task record.'),
  cers_wf_task_create_date DATE NOT NULL OPTIONS(description='The calendar date and time the task record was created.'),
  cers_wf_task_completed_by_uid BIGNUMERIC(38) OPTIONS(description='Identification number of the user that completed the workflow task.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY company_code, coid, cers_wf_task_id
OPTIONS(
  description='Reference table to calculation engine rate schedule workflow tasks.'
);
