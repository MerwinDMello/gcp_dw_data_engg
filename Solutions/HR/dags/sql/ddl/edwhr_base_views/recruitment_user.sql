create or replace view `{{ params.param_hr_base_views_dataset_name }}.recruitment_user`
AS SELECT
    recruitment_user.recruitment_user_sid,
    recruitment_user.valid_from_date,
    recruitment_user.recruitment_user_num,
    recruitment_user.valid_to_date,
    recruitment_user.employee_num,
    recruitment_user.employee_34_login_code,
    recruitment_user.first_name,
    recruitment_user.last_name,
    recruitment_user.source_system_code,
    recruitment_user.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.recruitment_user;