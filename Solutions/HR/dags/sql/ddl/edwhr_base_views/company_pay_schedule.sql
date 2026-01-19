create or replace view `{{ params.param_hr_base_views_dataset_name }}.company_pay_schedule`
AS SELECT
    company_pay_schedule.company_pay_schedule_sid,
    company_pay_schedule.eff_from_date,
    company_pay_schedule.valid_from_date,
    company_pay_schedule.valid_to_date,
    company_pay_schedule.hr_company_sid,
    company_pay_schedule.lawson_company_num,
    company_pay_schedule.pay_schedule_code,
    company_pay_schedule.pay_schedule_flag,
    company_pay_schedule.pay_schedule_eff_date,
    company_pay_schedule.pay_schedule_desc,
    company_pay_schedule.salary_class_flag,
    company_pay_schedule.last_grade_sequence_num,
    company_pay_schedule.pay_schedule_status_ind,
    company_pay_schedule.currency_code,
    company_pay_schedule.currency_nd,
    company_pay_schedule.active_dw_ind,
    company_pay_schedule.process_level_code,
    company_pay_schedule.security_key_text,
    company_pay_schedule.source_system_code,
    company_pay_schedule.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.company_pay_schedule;