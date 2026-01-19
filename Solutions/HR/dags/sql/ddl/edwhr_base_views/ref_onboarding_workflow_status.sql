CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow_status AS
	SELECT
    ref_onboarding_workflow_status.workflow_status_id,
    ref_onboarding_workflow_status.workflow_status_text,
    ref_onboarding_workflow_status.source_system_code,
    ref_onboarding_workflow_status.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow_status;