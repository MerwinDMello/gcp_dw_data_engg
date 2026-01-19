/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.fact_employee_availability AS SELECT
    a.employee_talent_profile_sid,
    a.valid_from_date,
    a.employee_sid,
    a.valid_to_date,
    a.jobs_pooled_for_cnt,
    a.employee_talent_pool_cnt,
    a.employee_successor_cnt,
    a.employee_ready_now_cnt,
    a.employee_ready_18_24_month_cnt,
    a.employee_ready_12_18_month_cnt,
    a.employee_ready_6_11_month_cnt,
    a.employee_other_readiness_cnt,
    a.employee_readiness_unknown_cnt,
    a.employee_slated_for_position_cnt,
    a.employee_talent_pooled_for_position_cnt,
    a.employee_num,
    a.lawson_company_num,
    a.process_level_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.fact_employee_availability AS a
;
