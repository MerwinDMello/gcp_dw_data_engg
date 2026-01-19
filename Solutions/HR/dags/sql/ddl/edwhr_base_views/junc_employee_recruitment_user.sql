CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.junc_employee_recruitment_user
AS SELECT
    junc_employee_recruitment_user.employee_sid,
    junc_employee_recruitment_user.recruitment_user_sid,
    junc_employee_recruitment_user.valid_from_date,
    junc_employee_recruitment_user.primary_facility_ind,
    junc_employee_recruitment_user.valid_to_date,
    junc_employee_recruitment_user.source_system_code,
    junc_employee_recruitment_user.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.junc_employee_recruitment_user;