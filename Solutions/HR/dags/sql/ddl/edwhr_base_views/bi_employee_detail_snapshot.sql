create or replace view `{{ params.param_hr_base_views_dataset_name }}.bi_employee_detail_snapshot`
AS SELECT
    bi_employee_detail_snapshot.employee_sid,
    bi_employee_detail_snapshot.snapshot_date,
    bi_employee_detail_snapshot.employee_num,
    bi_employee_detail_snapshot.employee_first_name,
    bi_employee_detail_snapshot.employee_last_name,
    bi_employee_detail_snapshot.employee_middle_name,
    bi_employee_detail_snapshot.ethnic_origin_code,
    bi_employee_detail_snapshot.gender_code,
    bi_employee_detail_snapshot.adjusted_hire_date,
    bi_employee_detail_snapshot.birth_date,
    bi_employee_detail_snapshot.acute_experience_start_date,
    bi_employee_detail_snapshot.lawson_company_num,
    bi_employee_detail_snapshot.process_level_code,
    bi_employee_detail_snapshot.source_system_code,
    bi_employee_detail_snapshot.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot;