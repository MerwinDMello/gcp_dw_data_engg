-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_cers_wf_task_owner.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner
(
  company_code STRING NOT NULL OPTIONS(description=' Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description=' The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  cers_wf_task_owner_id BIGNUMERIC(38) NOT NULL OPTIONS(description=' Primary key for each corresponding EDWRA_STAGING.Cers_workflow_task_Owner row'),
  cers_wf_task_id BIGNUMERIC(38) NOT NULL OPTIONS(description=' Foreign key to cers_workflow_task.id'),
  cers_wf_task_owner_uid BIGNUMERIC(38) OPTIONS(description=' User identification number of the owner of the task.'),
  cers_wf_task_owner_name STRING OPTIONS(description=' The name of the owner of the task.'),
  cers_wf_task_owner_email_addr STRING OPTIONS(description=' The email address of the owner of the task.'),
  cers_wf_task_owner_cplt_ind STRING NOT NULL OPTIONS(description=' The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  cers_wf_task_owner_is_notf_ind STRING NOT NULL OPTIONS(description=' The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  cers_wf_task_ownr_cplt_date DATE OPTIONS(description=' The calendar date and time of when the owner completed the task.'),
  cers_wf_task_ownr_chck_off_ind STRING OPTIONS(description=' The indicator is usually a Y/N (yes/no) designation of a record but can sometimes be other values.  This should not be 1/0 designation.'),
  source_system_code STRING NOT NULL OPTIONS(description=' A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description=' Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY company_code, coid, cers_wf_task_owner_id
OPTIONS(
  description='Reference table to owner of calculation engine rate schedule workflow tasks.'
);
