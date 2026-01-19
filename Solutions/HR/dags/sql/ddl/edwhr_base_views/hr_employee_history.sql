create or replace view `{{ params.param_hr_base_views_dataset_name }}.hr_employee_history`
AS SELECT
  hr_employee_history.employee_sid,
  hr_employee_history.lawson_element_num,
  hr_employee_history.hr_employee_element_date,
  hr_employee_history.lawson_object_id,
  hr_employee_history.sequence_num,
  hr_employee_history.valid_from_date,
  hr_employee_history.valid_to_date,
  hr_employee_history.hr_employee_value_num,
  hr_employee_history.hr_employee_value_alphanumeric_text,
  hr_employee_history.hr_employee_value_date,
  hr_employee_history.action_object_identifier,
  hr_employee_history.user_3_4_login_code,
  hr_employee_history.data_type_flag,
  hr_employee_history.position_level_sequence_num,
  hr_employee_history.last_update_date,
  hr_employee_history.last_update_time,
  hr_employee_history.employee_num,
  hr_employee_history.lawson_company_num,
  hr_employee_history.process_level_code,
  hr_employee_history.delete_ind,
  hr_employee_history.source_system_code,
  hr_employee_history.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.hr_employee_history;