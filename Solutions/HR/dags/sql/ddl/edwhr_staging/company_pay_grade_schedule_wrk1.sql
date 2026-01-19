create table if not exists {{ params.param_hr_stage_dataset_name }}.company_pay_grade_schedule_wrk1 (
company_pay_schedule_sid int64 not null
, pay_grade_code string not null
, pay_step_num int64 not null
, eff_from_date date not null
, valid_from_date datetime not null
, eff_to_date date
, pay_schedule_code string
, grade_sequence_num int64
, pay_rate_amt numeric
, lawson_company_num int64 not null
, process_level_code string
, active_dw_ind string
, source_system_code string
, dw_last_update_date_time datetime
)
  ;
