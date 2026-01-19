create or replace view `{{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history`
AS SELECT
    recruitment_requisition_history.recruitment_requisition_sid,
    recruitment_requisition_history.creation_date_time,
    recruitment_requisition_history.requisition_status_id,
    recruitment_requisition_history.valid_from_date,
    recruitment_requisition_history.valid_to_date,
    recruitment_requisition_history.closed_date_time,
    recruitment_requisition_history.requisition_creator_user_sid,
    recruitment_requisition_history.recruiter_owner_user_sid,
    recruitment_requisition_history.source_system_code,
    recruitment_requisition_history.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.recruitment_requisition_history;