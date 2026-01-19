create or replace view `{{ params.param_hr_base_views_dataset_name }}.requisition_workflow`
AS SELECT
  requisition_workflow.requisition_sid,
  requisition_workflow.workflow_seq_num,
  requisition_workflow.valid_from_date,
  requisition_workflow.valid_to_date,
  requisition_workflow.workflow_workunit_num_text,
  requisition_workflow.workflow_activity_num,
  requisition_workflow.workflow_role_name,
  requisition_workflow.workflow_task_name,
  requisition_workflow.start_date,
  requisition_workflow.start_time,
  requisition_workflow.end_date,
  requisition_workflow.end_time,
  requisition_workflow.workflow_user_id_code,
  requisition_workflow.lawson_company_num,
  requisition_workflow.process_level_code,
  requisition_workflow.active_dw_ind,
  requisition_workflow.source_system_code,
  requisition_workflow.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.requisition_workflow;