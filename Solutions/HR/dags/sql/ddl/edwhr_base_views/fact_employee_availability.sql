CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.fact_employee_availability
AS SELECT
    fact_employee_availability.employee_talent_profile_sid,
    fact_employee_availability.valid_from_date,
    fact_employee_availability.employee_sid,
    fact_employee_availability.valid_to_date,
    fact_employee_availability.jobs_pooled_for_cnt,
    fact_employee_availability.employee_talent_pool_cnt,
    fact_employee_availability.employee_successor_cnt,
    fact_employee_availability.employee_ready_now_cnt,
    fact_employee_availability.employee_ready_18_24_month_cnt,
    fact_employee_availability.employee_ready_12_18_month_cnt,
    fact_employee_availability.employee_ready_6_11_month_cnt,
    fact_employee_availability.employee_other_readiness_cnt,
    fact_employee_availability.employee_readiness_unknown_cnt,
    fact_employee_availability.employee_slated_for_position_cnt,
    fact_employee_availability.employee_talent_pooled_for_position_cnt,
    fact_employee_availability.employee_num,
    fact_employee_availability.lawson_company_num,
    fact_employee_availability.process_level_code,
    fact_employee_availability.source_system_code,
    fact_employee_availability.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.fact_employee_availability;