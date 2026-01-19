CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.company_pay_grade_schedule AS SELECT
    company_pay_grade_schedule.company_pay_schedule_sid,
    company_pay_grade_schedule.pay_grade_code,
    company_pay_grade_schedule.pay_step_num,
    company_pay_grade_schedule.eff_from_date,
    company_pay_grade_schedule.valid_from_date,
    company_pay_grade_schedule.valid_to_date,
    company_pay_grade_schedule.pay_schedule_code,
    company_pay_grade_schedule.grade_sequence_num,
    company_pay_grade_schedule.pay_rate_amt,
    company_pay_grade_schedule.lawson_company_num,
    company_pay_grade_schedule.process_level_code,
    company_pay_grade_schedule.active_dw_ind,
    company_pay_grade_schedule.source_system_code,
    company_pay_grade_schedule.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule
;