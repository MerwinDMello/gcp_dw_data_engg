create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_workflow`
AS SELECT
    ref_workflow.workflow_id,
    ref_workflow.active_sw,
    ref_workflow.workflow_code,
    ref_workflow.workflow_name,
    ref_workflow.workflow_desc,
    ref_workflow.source_system_code,
    ref_workflow.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_workflow;