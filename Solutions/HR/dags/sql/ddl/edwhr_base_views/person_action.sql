create or replace view `{{ params.param_hr_base_views_dataset_name }}.person_action`
AS SELECT
    person_action.person_action_sid,
    person_action.eff_from_date,
    person_action.valid_from_date,
    person_action.valid_to_date,
    person_action.action_code,
    person_action.employee_sid,
    person_action.applicant_sid,
    person_action.employee_num,
    person_action.applicant_num,
    person_action.action_type_code,
    person_action.action_sequence_num,
    person_action.action_from_date,
    person_action.action_to_date,
    person_action.requisition_sid,
    person_action.action_reason_text,
    person_action.person_action_update_sid,
    person_action.person_action_flag,
    person_action.action_last_update_date,
    person_action.active_dw_ind,
    person_action.delete_ind,
    person_action.lawson_company_num,
    person_action.process_level_code,
    person_action.source_system_code,
    person_action.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.person_action;