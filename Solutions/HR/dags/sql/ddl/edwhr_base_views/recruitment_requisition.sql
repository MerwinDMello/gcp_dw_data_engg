create or replace view `{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition`
AS SELECT
    recruitment_requisition.recruitment_requisition_sid,
    recruitment_requisition.valid_from_date,
    recruitment_requisition.valid_to_date,
    recruitment_requisition.requisition_num,
    recruitment_requisition.lawson_requisition_sid,
    recruitment_requisition.lawson_requisition_num,
    recruitment_requisition.hiring_manager_user_sid,
    recruitment_requisition.recruitment_requisition_num_text,
    recruitment_requisition.process_level_code,
    recruitment_requisition.approved_sw,
    recruitment_requisition.target_start_date,
    recruitment_requisition.required_asset_num,
    recruitment_requisition.required_asset_sw,
    recruitment_requisition.workflow_id,
    recruitment_requisition.recruitment_job_sid,
    recruitment_requisition.job_template_sid,
    recruitment_requisition.requisition_new_graduate_flag,
    recruitment_requisition.lawson_company_num,
    recruitment_requisition.source_system_code,
    recruitment_requisition.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.recruitment_requisition;